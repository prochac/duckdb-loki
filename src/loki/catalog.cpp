#include "loki/catalog.hpp"

#include "loki/connection.hpp"
#include "loki/http_client.hpp"
#include "loki/http_request.hpp"
#include "loki/logql.hpp"
#include "loki/loki_discovery.hpp"
#include "loki/loki_scan.hpp"
#include "loki/parse.hpp"
#include "loki/time_bounds.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

namespace duckdb {

namespace {

[[noreturn]] void ThrowReadOnly() {
	throw NotImplementedException("loki catalog is read-only: ATTACH exposes Loki for querying only "
	                              "(no CREATE/INSERT/UPDATE/DELETE)");
}

// Parse an ATTACH option into a list of strings, accepting either a LIST value
// (`labels=['job','level']`) or a comma-separated VARCHAR (`labels='job,level'`). Absent → empty.
vector<string> ParseStringListOption(const unordered_map<string, Value> &options, const char *key) {
	vector<string> result;
	auto it = options.find(key);
	if (it == options.end() || it->second.IsNull()) {
		return result;
	}
	const Value &value = it->second;
	if (value.type().id() == LogicalTypeId::LIST) {
		for (const auto &child : ListValue::GetChildren(value)) {
			if (!child.IsNull()) {
				result.push_back(child.ToString());
			}
		}
	} else {
		for (auto &part : StringUtil::Split(value.ToString(), ',')) {
			StringUtil::Trim(part);
			if (!part.empty()) {
				result.push_back(part);
			}
		}
	}
	return result;
}

// Parse the ATTACH `label_cache_ttl` option into nanoseconds (DESIGN.md §11.8). Accepts an
// INTERVAL, an integer count of seconds, or a duration string ('0', '30s', '5m', '1h'). Absent → 0,
// i.e. no time-based refresh at all.
int64_t ParseTtlOption(const unordered_map<string, Value> &options, const char *key) {
	auto it = options.find(key);
	if (it == options.end() || it->second.IsNull()) {
		return 0;
	}
	const Value &value = it->second;
	try {
		if (value.type().id() == LogicalTypeId::INTERVAL) {
			auto interval = IntervalValue::Get(value);
			int64_t nanos = loki::IntervalToNanos(interval.months, interval.days, interval.micros);
			if (nanos < 0) {
				throw std::invalid_argument("negative");
			}
			return nanos;
		}
		if (value.type().IsIntegral()) {
			int64_t seconds = value.GetValue<int64_t>();
			if (seconds < 0) {
				throw std::invalid_argument("negative");
			}
			int64_t nanos = 0;
			if (!loki::TryMultiplyNanos(seconds, 1000LL * 1000LL * 1000LL, nanos)) {
				throw std::out_of_range("too large");
			}
			return nanos;
		}
		return loki::ParseDurationToNanos(value.ToString());
	} catch (const std::exception &e) {
		throw BinderException("ATTACH (TYPE loki): invalid `%s` value '%s' — pass a non-negative INTERVAL, a number of "
		                      "seconds, or a duration string like '30s', '5m', '1h' (0 disables time-based refresh)",
		                      key, value.ToString());
	}
}

// Auto-discover the promoted label columns for a tenant by calling GET /labels at attach time
// (DESIGN.md §3.6). Used only when the ATTACH `labels` option is absent — an explicit `labels`
// (even an empty list) opts out. Errors surface clearly so an unreachable Loki fails the ATTACH.
vector<string> FetchLokiLabelNames(const std::string &endpoint, const loki::AuthConfig &auth,
                                   int connect_timeout_s = 10, int read_timeout_s = 60) {
	auto request = loki::BuildLabelsRequest(-1, -1); // default window: let Loki decide
	auto headers = loki::BuildAuthHeaders(auth);
	auto query_string = loki::BuildQueryString(request);
	auto response = loki::HttpGet(endpoint, request.path, query_string, headers, connect_timeout_s, read_timeout_s);
	if (!response.error.empty()) {
		throw IOException("ATTACH (TYPE loki): label auto-discovery request to Loki failed: " + response.error);
	}
	if (response.status < 200 || response.status >= 300) {
		throw IOException(StringUtil::Format("ATTACH (TYPE loki): label auto-discovery got HTTP %d: %s",
		                                     response.status, loki::ResponseSnippet(response.body, 1000)));
	}
	vector<string> discovered;
	try {
		discovered = loki::ParseStringArrayResponse(response.body);
	} catch (const std::exception &e) {
		throw IOException(std::string("ATTACH (TYPE loki): label auto-discovery: ") + e.what());
	}
	// These names are server-controlled — Loki derives them from ingested data — and they become
	// both DuckDB column names and (unescaped) LogQL matcher names in every later query. Drop
	// anything outside the label grammar instead of letting a polluted Loki steer our LogQL.
	// Names that would collide with the fixed schema columns, or with each other, are dropped for
	// the same reason: otherwise a Loki carrying a label called `line` would make ATTACH itself
	// fail with DuckDB's "Column with name line already exists!". Those labels remain readable
	// through the `labels` MAP; they just are not promoted.
	vector<string> valid;
	case_insensitive_set_t seen;
	for (auto &name : discovered) {
		if (!loki::IsValidLabelName(name) || LokiIsReservedColumnName(name)) {
			continue;
		}
		if (!seen.insert(name).second) {
			continue;
		}
		valid.push_back(std::move(name));
	}
	return valid;
}

// Add the `series` table's columns: one VARCHAR per promoted label, then the full labels MAP.
void BuildLokiSeriesColumns(const std::vector<std::string> &label_columns, ColumnList &columns) {
	for (const auto &label : label_columns) {
		columns.AddColumn(ColumnDefinition(Identifier(label), LogicalType::VARCHAR));
	}
	columns.AddColumn(ColumnDefinition("labels", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)));
}

// Global state for the `series` scan: the fetched series label-sets, streamed out per chunk.
struct LokiSeriesScanGlobalState : public GlobalTableFunctionState {
	LokiSeriesScanGlobalState(std::string endpoint_p, std::vector<std::pair<std::string, std::string>> headers_p,
	                          loki::QueryRangeRequest request_p)
	    : endpoint(std::move(endpoint_p)), headers(std::move(headers_p)), request(std::move(request_p)) {
	}
	std::string endpoint;
	std::vector<std::pair<std::string, std::string>> headers;
	loki::QueryRangeRequest request;
	bool fetched = false;
	std::vector<std::map<std::string, std::string>> rows;
	idx_t offset = 0;
	std::vector<column_t> column_ids;

	idx_t MaxThreads() const override {
		return 1;
	}
};

// init for the `series` scan: after pushdown has populated the matchers, synthesize the `match[]`
// selector and build the /series request. Reuses the same mandatory-selector rule as `logs`.
unique_ptr<GlobalTableFunctionState> LokiSeriesScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<LokiScanBindData>();
	std::string match = loki::BuildSelectorQuery(bind_data.base_selector, bind_data.pushed_matchers, {});
	if (match.size() >= 2 && match[0] == '{' && match[1] == '}') {
		throw BinderException("%s: a stream selector is required — add a WHERE equality/IN on a label column "
		                      "(Loki refuses /series without a selector)",
		                      bind_data.fn_name);
	}
	auto headers = loki::BuildAuthHeaders(bind_data.auth);
	auto request = loki::BuildSeriesRequest(match, bind_data.start_ns, bind_data.end_ns);
	auto state = make_uniq<LokiSeriesScanGlobalState>(bind_data.endpoint, std::move(headers), std::move(request));
	state->column_ids = input.column_ids;
	return std::move(state);
}

void LokiSeriesScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<LokiSeriesScanGlobalState>();
	auto &bind_data = data_p.bind_data->Cast<LokiScanBindData>();

	if (!state.fetched) {
		auto query_string = loki::BuildQueryString(state.request);
		auto response = loki::HttpGet(state.endpoint, state.request.path, query_string, state.headers);
		if (!response.error.empty()) {
			throw IOException(bind_data.fn_name + ": request to Loki failed: " + response.error);
		}
		if (response.status < 200 || response.status >= 300) {
			throw IOException(StringUtil::Format("%s: Loki returned HTTP %d: %s", bind_data.fn_name, response.status,
			                                     loki::ResponseSnippet(response.body, 1000)));
		}
		try {
			state.rows = loki::ParseSeriesResponse(response.body);
		} catch (const std::exception &e) {
			throw IOException(bind_data.fn_name + ": " + e.what());
		}
		// Loki's /series takes neither a limit nor a cursor, so a broad selector can only be
		// answered by materializing every matching series. Refuse instead of silently ballooning.
		if (state.rows.size() > bind_data.series_max_rows) {
			throw IOException(
			    "%s: the selector matched %llu series, over the %llu-row cap — narrow the WHERE clause or the time "
			    "window, or raise it with ATTACH ... (TYPE loki, series_max_rows N). Loki's /series endpoint cannot "
			    "page, so the whole result must be held in memory",
			    bind_data.fn_name, static_cast<uint64_t>(state.rows.size()),
			    static_cast<uint64_t>(bind_data.series_max_rows));
		}
		state.fetched = true;
	}

	// Source-column meaning: [0, N) = promoted label column, N = labels MAP (N = #promoted labels).
	const idx_t num_labels = bind_data.label_columns.size();
	const column_t labels_map_col = num_labels;

	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE && state.offset < state.rows.size()) {
		const auto &row = state.rows[state.offset];
		for (idx_t out = 0; out < state.column_ids.size(); out++) {
			const column_t src = state.column_ids[out];
			if (src == COLUMN_IDENTIFIER_ROW_ID || IsVirtualColumn(src)) {
				// NULL, not skip: SetCardinality still counts this row, so an untouched slot would
				// be an uninitialized flat-vector entry (see the same guard in LokiScanFunction).
				FlatVector::SetNull(output.data[out], count, true);
				continue;
			}
			auto &vec = output.data[out];
			if (src < labels_map_col) {
				const auto &name = bind_data.label_columns[src];
				auto it = row.find(name);
				vec.SetValue(count, it != row.end() ? Value(it->second) : Value(LogicalType::VARCHAR));
			} else {
				vec.SetValue(count, LokiMapFromStrings(row));
			}
		}
		state.offset++;
		count++;
	}
	output.SetChildCardinality(count);
}

} // namespace

//===--------------------------------------------------------------------===//
// LokiCatalog
//===--------------------------------------------------------------------===//

LokiCatalog::LokiCatalog(AttachedDatabase &db, LokiCatalogConfig config_p) : Catalog(db), config(std::move(config_p)) {
	// Never leave `current` null: a lookup arriving before Initialize should see an empty catalog,
	// not dereference nothing.
	current = make_shared_ptr<const LokiCatalogSnapshot>();
	next_check_ns.store(std::numeric_limits<int64_t>::max());
}

LokiCatalog::~LokiCatalog() = default;

shared_ptr<const LokiCatalogSnapshot> LokiCatalog::BuildSnapshot(std::vector<std::vector<std::string>> labels,
                                                                 idx_t version) {
	auto snapshot = make_uniq<LokiCatalogSnapshot>();
	snapshot->version = version;
	snapshot->label_columns = std::move(labels);
	for (idx_t i = 0; i < config.tenants.size(); i++) {
		LokiTenant tenant = config.tenants[i];
		if (i < snapshot->label_columns.size()) {
			tenant.label_columns = snapshot->label_columns[i];
		}
		CreateSchemaInfo schema_info;
		schema_info.SetSchema(Identifier(tenant.schema_name));
		snapshot->schemas.push_back(make_uniq<LokiSchemaEntry>(*this, schema_info, config, tenant));
	}
	return shared_ptr<const LokiCatalogSnapshot>(std::move(snapshot));
}

void LokiCatalog::Initialize(bool load_builtin) {
	// Build generation 0 eagerly: it is fully determined by the attach-time config (endpoint, auth,
	// per-tenant label columns), so no Loki round-trip happens here.
	std::vector<std::vector<std::string>> labels;
	labels.reserve(config.tenants.size());
	for (const auto &tenant : config.tenants) {
		labels.push_back(tenant.label_columns);
	}
	auto snapshot = BuildSnapshot(std::move(labels), 0);
	// The attach-time fetch counts as the first check, so the TTL clock starts now rather than
	// making the very first statement re-fetch.
	int64_t now_ns = Timestamp::GetEpochNanoSeconds(Timestamp::GetCurrentTimestamp());
	next_check_ns.store(now_ns + config.label_cache_ttl_ns);
	lock_guard<mutex> guard(snapshot_lock);
	current = std::move(snapshot);
}

shared_ptr<const LokiCatalogSnapshot> LokiCatalog::PinCurrentSnapshot() {
	lock_guard<mutex> guard(snapshot_lock);
	return current;
}

bool LokiCatalog::DoRefresh(bool short_timeouts) {
	// A refresh on the TTL path runs on the transaction-start path, so it must fail fast rather
	// than stall a connection for the full default read timeout.
	const int connect_timeout_s = short_timeouts ? 5 : 10;
	const int read_timeout_s = short_timeouts ? 5 : 60;

	// Fetch every tenant BEFORE touching any state: a tenant that fails must abort the whole
	// refresh, never leave the catalog half-updated.
	std::vector<std::vector<std::string>> fetched;
	fetched.reserve(config.tenants.size());
	for (const auto &tenant : config.tenants) {
		loki::AuthConfig tenant_auth = config.auth;
		if (!tenant.tenant_id.empty()) {
			tenant_auth.tenant = tenant.tenant_id;
		}
		fetched.push_back(FetchLokiLabelNames(config.endpoint, tenant_auth, connect_timeout_s, read_timeout_s));
	}

	idx_t next_version = 0;
	{
		lock_guard<mutex> guard(snapshot_lock);
		// Unchanged labels must NOT publish a new generation: new catalog entries mean new oids and
		// a forced re-bind of every prepared statement, which a periodic TTL would then inflict
		// over and over on a Loki whose label set never moves.
		if (current && current->label_columns == fetched) {
			return false;
		}
		next_version = current ? current->version + 1 : 0;
	}

	// Built outside the lock: this allocates a full schema tree.
	auto snapshot = BuildSnapshot(fetched, next_version);

	lock_guard<mutex> guard(snapshot_lock);
	retired.push_back(current);
	current = std::move(snapshot);
	// Drop superseded generations no transaction is reading any more.
	for (idx_t i = retired.size(); i > 0; i--) {
		if (retired[i - 1].use_count() == 1) {
			retired.erase(retired.begin() + static_cast<int64_t>(i - 1));
		}
	}
	return true;
}

void LokiCatalog::MaybeRefreshLabels(ClientContext &context) {
	if (config.labels_pinned || config.label_cache_ttl_ns == 0) {
		return;
	}
	int64_t now_ns = Timestamp::GetEpochNanoSeconds(Timestamp::GetCurrentTimestamp());
	if (now_ns < next_check_ns.load()) {
		return;
	}
	// try_lock, not lock: if another connection is already refreshing, serve the current snapshot
	// rather than queueing this transaction behind an HTTP round-trip.
	unique_lock<mutex> refresh_guard(refresh_lock, std::try_to_lock);
	if (!refresh_guard.owns_lock()) {
		return;
	}
	if (now_ns < next_check_ns.load()) {
		return; // another connection refreshed while we waited
	}
	try {
		DoRefresh(/*short_timeouts=*/true);
		next_check_ns.store(now_ns + config.label_cache_ttl_ns);
	} catch (const std::exception &) {
		// Deliberately total (DESIGN.md §11.8): a Loki outage must not make an attached database
		// unqueryable at bind time. Keep the old snapshot and back off before trying again.
		constexpr int64_t REFRESH_BACKOFF_NS = 60LL * 1000LL * 1000LL * 1000LL;
		next_check_ns.store(now_ns + std::max(config.label_cache_ttl_ns, REFRESH_BACKOFF_NS));
	}
}

void LokiCatalog::RefreshLabels(ClientContext &context) {
	if (config.labels_pinned) {
		throw InvalidInputException(
		    "loki catalog '%s' was attached with an explicit `labels` option, so its promoted columns are pinned and "
		    "there is nothing to refresh; re-ATTACH without `labels` to auto-discover them",
		    GetName());
	}
	lock_guard<mutex> guard(refresh_lock);
	DoRefresh(/*short_timeouts=*/false); // errors surface: the user asked for this explicitly
	int64_t now_ns = Timestamp::GetEpochNanoSeconds(Timestamp::GetCurrentTimestamp());
	next_check_ns.store(now_ns + config.label_cache_ttl_ns);
}

shared_ptr<const LokiCatalogSnapshot> LokiCatalog::PinnedSnapshot(optional_ptr<Transaction> transaction) {
	// Any non-null transaction reaching this catalog was started by our own transaction manager
	// (Transaction::Get resolves per attached database), so it always carries a pin. A null one is
	// the system catalog transaction (CatalogTransaction::GetSystemCatalogTransaction).
	if (transaction) {
		auto &loki_transaction = transaction->Cast<LokiTransaction>();
		if (loki_transaction.snapshot) {
			return loki_transaction.snapshot;
		}
	}
	return PinCurrentSnapshot();
}

optional_ptr<SchemaCatalogEntry> LokiCatalog::LookupSchema(CatalogTransaction transaction,
                                                           const EntryLookupInfo &schema_lookup,
                                                           OnEntryNotFound if_not_found) {
	const auto &name = schema_lookup.GetEntryName();
	auto snapshot = PinnedSnapshot(transaction.transaction);
	for (auto &schema : snapshot->schemas) {
		if (StringUtil::CIEquals(schema->name.GetIdentifierName(), name)) {
			return schema.get();
		}
	}
	if (if_not_found == OnEntryNotFound::RETURN_NULL) {
		return nullptr;
	}
	throw CatalogException("Schema '%s' not found in loki catalog '%s'", name, GetName());
}

void LokiCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	auto snapshot = PinnedSnapshot(&Transaction::Get(context, GetAttached()));
	for (auto &schema : snapshot->schemas) {
		callback(*schema);
	}
}

optional<Identifier> LokiCatalog::GetDefaultSchema() const {
	return Identifier(config.tenants.empty() ? string("public") : config.tenants.front().schema_name);
}

optional_ptr<CatalogEntry> LokiCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	ThrowReadOnly();
}
void LokiCatalog::DropSchema(ClientContext &, DropInfo &) {
	ThrowReadOnly();
}
PhysicalOperator &LokiCatalog::PlanCreateTableAs(ClientContext &, PhysicalPlanGenerator &, LogicalCreateTable &,
                                                 PhysicalOperator &) {
	ThrowReadOnly();
}
PhysicalOperator &LokiCatalog::PlanInsert(ClientContext &, PhysicalPlanGenerator &, LogicalInsert &,
                                          optional_ptr<PhysicalOperator>) {
	ThrowReadOnly();
}
PhysicalOperator &LokiCatalog::PlanDelete(ClientContext &, PhysicalPlanGenerator &, LogicalDelete &,
                                          PhysicalOperator &) {
	ThrowReadOnly();
}
PhysicalOperator &LokiCatalog::PlanUpdate(ClientContext &, PhysicalPlanGenerator &, LogicalUpdate &,
                                          PhysicalOperator &) {
	ThrowReadOnly();
}

//===--------------------------------------------------------------------===//
// LokiSchemaEntry
//===--------------------------------------------------------------------===//

LokiSchemaEntry::LokiSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, const LokiCatalogConfig &catalog_config,
                                 const LokiTenant &tenant)
    : SchemaCatalogEntry(catalog, info) {
	// Per-tenant auth: base auth from the secret/inline options, with this tenant's X-Scope-OrgID.
	loki::AuthConfig auth = catalog_config.auth;
	if (!tenant.tenant_id.empty()) {
		auth.tenant = tenant.tenant_id;
	}

	CreateTableInfo logs_info(QualifiedName(catalog.GetName(), Identifier(tenant.schema_name), "logs"));
	BuildLokiLogsColumns(tenant.label_columns, logs_info.columns);
	logs_table =
	    make_uniq<LokiLogsTable>(catalog, *this, logs_info, catalog_config.endpoint, auth, tenant.label_columns);

	// Optional exploration tables (DESIGN.md §3.6): `labels` (all label names) and `series`
	// (matching series, selector-driven like `logs`).
	CreateTableInfo labels_info(QualifiedName(catalog.GetName(), Identifier(tenant.schema_name), "labels"));
	labels_info.columns.AddColumn(ColumnDefinition("label", LogicalType::VARCHAR));
	labels_table = make_uniq<LokiLabelsTable>(catalog, *this, labels_info, catalog_config.endpoint, auth);

	CreateTableInfo series_info(QualifiedName(catalog.GetName(), Identifier(tenant.schema_name), "series"));
	BuildLokiSeriesColumns(tenant.label_columns, series_info.columns);
	series_table = make_uniq<LokiSeriesTable>(catalog, *this, series_info, catalog_config.endpoint, std::move(auth),
	                                          tenant.label_columns, catalog_config.series_max_rows);
}

void LokiSchemaEntry::Scan(ClientContext &context, CatalogType type,
                           const std::function<void(CatalogEntry &)> &callback) {
	if (type == CatalogType::TABLE_ENTRY) {
		callback(*logs_table);
		callback(*labels_table);
		callback(*series_table);
	}
}
void LokiSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	if (type == CatalogType::TABLE_ENTRY) {
		callback(*logs_table);
		callback(*labels_table);
		callback(*series_table);
	}
}

optional_ptr<CatalogEntry> LokiSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                        const EntryLookupInfo &lookup_info) {
	if (lookup_info.GetCatalogType() != CatalogType::TABLE_ENTRY) {
		return nullptr;
	}
	const auto &name = lookup_info.GetEntryName();
	if (StringUtil::CIEquals(name, "logs")) {
		return logs_table.get();
	}
	if (StringUtil::CIEquals(name, "labels")) {
		return labels_table.get();
	}
	if (StringUtil::CIEquals(name, "series")) {
		return series_table.get();
	}
	return nullptr;
}

optional_ptr<CatalogEntry> LokiSchemaEntry::CreateTable(CatalogTransaction, BoundCreateTableInfo &) {
	ThrowReadOnly();
}
optional_ptr<CatalogEntry> LokiSchemaEntry::CreateFunction(CatalogTransaction, CreateFunctionInfo &) {
	ThrowReadOnly();
}
optional_ptr<CatalogEntry> LokiSchemaEntry::CreateIndex(CatalogTransaction, CreateIndexInfo &, TableCatalogEntry &) {
	ThrowReadOnly();
}
optional_ptr<CatalogEntry> LokiSchemaEntry::CreateView(CatalogTransaction, CreateViewInfo &) {
	ThrowReadOnly();
}
optional_ptr<CatalogEntry> LokiSchemaEntry::CreateSequence(CatalogTransaction, CreateSequenceInfo &) {
	ThrowReadOnly();
}
optional_ptr<CatalogEntry> LokiSchemaEntry::CreateTableFunction(CatalogTransaction, CreateTableFunctionInfo &) {
	ThrowReadOnly();
}
optional_ptr<CatalogEntry> LokiSchemaEntry::CreateCopyFunction(CatalogTransaction, CreateCopyFunctionInfo &) {
	ThrowReadOnly();
}
optional_ptr<CatalogEntry> LokiSchemaEntry::CreatePragmaFunction(CatalogTransaction, CreatePragmaFunctionInfo &) {
	ThrowReadOnly();
}
optional_ptr<CatalogEntry> LokiSchemaEntry::CreateCollation(CatalogTransaction, CreateCollationInfo &) {
	ThrowReadOnly();
}
optional_ptr<CatalogEntry> LokiSchemaEntry::CreateType(CatalogTransaction, CreateTypeInfo &) {
	ThrowReadOnly();
}
void LokiSchemaEntry::DropEntry(ClientContext &, DropInfo &) {
	ThrowReadOnly();
}
void LokiSchemaEntry::Alter(CatalogTransaction, AlterInfo &) {
	ThrowReadOnly();
}

//===--------------------------------------------------------------------===//
// LokiLogsTable
//===--------------------------------------------------------------------===//

LokiLogsTable::LokiLogsTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                             std::string endpoint_p, loki::AuthConfig auth_p, std::vector<std::string> label_columns_p)
    : TableCatalogEntry(catalog, schema, info), columns(info.columns.Copy()), endpoint(std::move(endpoint_p)),
      auth(std::move(auth_p)), label_columns(std::move(label_columns_p)) {
}

TableFunction LokiLogsTable::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto bd = make_uniq<LokiScanBindData>();
	bd->pushdown_mode = true; // WHERE synthesizes the selector; the mandatory-selector rule
	bd->base_selector = "";   // (DESIGN.md §4.3) is enforced at scan start, so a bare scan errors.
	bd->endpoint = endpoint;
	bd->auth = auth;
	bd->label_columns = label_columns;
	// The catalog has no `selector :=` argument, so name the fix the user actually has.
	bd->fn_name = "loki logs";
	bd->selector_hint = "add a WHERE equality/IN on a label column, e.g. WHERE job = 'api'";
	// The catalog path bypasses LokiResolveCommon, where the function path sets the default window;
	// set the last-hour default here (DESIGN.md §6.6) so a query without a timestamp WHERE is bounded.
	int64_t now_ns = Timestamp::GetEpochNanoSeconds(Timestamp::GetCurrentTimestamp());
	bd->end_ns = now_ns;
	bd->start_ns = now_ns - LOKI_NANOS_PER_HOUR;
	bind_data = std::move(bd);
	return MakeLokiCatalogScanFunction();
}

//===--------------------------------------------------------------------===//
// LokiLabelsTable
//===--------------------------------------------------------------------===//

LokiLabelsTable::LokiLabelsTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                 std::string endpoint_p, loki::AuthConfig auth_p)
    : TableCatalogEntry(catalog, schema, info), columns(info.columns.Copy()), endpoint(std::move(endpoint_p)),
      auth(std::move(auth_p)) {
}

TableFunction LokiLabelsTable::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto bd = make_uniq<LokiDiscoveryBindData>();
	bd->kind = DiscoveryKind::LABELS;
	bd->endpoint = endpoint;
	bd->auth = auth;
	bd->request = loki::BuildLabelsRequest(-1, -1); // default window: let Loki decide
	bind_data = std::move(bd);
	return MakeLokiDiscoveryScanFunction();
}

//===--------------------------------------------------------------------===//
// LokiSeriesTable
//===--------------------------------------------------------------------===//

LokiSeriesTable::LokiSeriesTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                 std::string endpoint_p, loki::AuthConfig auth_p,
                                 std::vector<std::string> label_columns_p, idx_t max_rows_p)
    : TableCatalogEntry(catalog, schema, info), columns(info.columns.Copy()), endpoint(std::move(endpoint_p)),
      auth(std::move(auth_p)), label_columns(std::move(label_columns_p)), max_rows(max_rows_p) {
}

TableFunction LokiSeriesTable::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto bd = make_uniq<LokiScanBindData>();
	bd->pushdown_mode = true; // WHERE label predicates synthesize the match[] selector.
	bd->base_selector = "";
	bd->endpoint = endpoint;
	bd->auth = auth;
	bd->label_columns = label_columns;
	bd->fn_name = "loki series";
	bd->series_max_rows = max_rows;
	int64_t now_ns = Timestamp::GetEpochNanoSeconds(Timestamp::GetCurrentTimestamp());
	bd->end_ns = now_ns;
	bd->start_ns = now_ns - LOKI_NANOS_PER_HOUR;
	bind_data = std::move(bd);
	return MakeLokiPushdownTableFunction(LokiSeriesScanFunction, LokiSeriesScanInitGlobal);
}

//===--------------------------------------------------------------------===//
// StorageExtension wiring
//===--------------------------------------------------------------------===//

unique_ptr<Catalog> LokiAttach(optional_ptr<StorageExtensionInfo>, ClientContext &context, AttachedDatabase &db,
                               const string &name, AttachInfo &info, AttachOptions &options) {
	LokiCatalogConfig config;
	ResolveLokiConnectionFromOptions(context, info.path, info.options, config.endpoint, config.auth,
	                                 "ATTACH (TYPE loki)");

	// Promoted label columns (DESIGN.md §4.2, §3.6). An explicit `labels` option (even an empty
	// list) pins the columns for every schema; when it is absent we auto-discover per tenant via
	// GET /labels — the "both" behavior chosen for ATTACH, where a bare attach is still usable.
	bool labels_explicit = info.options.find("labels") != info.options.end();
	auto explicit_labels = ParseStringListOption(info.options, "labels");
	case_insensitive_set_t seen_labels;
	for (const auto &label : explicit_labels) {
		// Promoted label names go into the LogQL selector unescaped; reject anything that is not a
		// label name outright (auto-discovered names are filtered instead, in FetchLokiLabelNames).
		if (!loki::IsValidLabelName(label)) {
			throw BinderException("ATTACH (TYPE loki): invalid label name '%s' in the `labels` option; label names "
			                      "must match [a-zA-Z_][a-zA-Z0-9_]*",
			                      label);
		}
		if (LokiIsReservedColumnName(label)) {
			throw BinderException("ATTACH (TYPE loki): label '%s' cannot be promoted — that name is already one of "
			                      "the fixed `logs` columns (timestamp, line, labels, structured_metadata); read it "
			                      "from the labels MAP instead, e.g. labels['%s']",
			                      label, label);
		}
		if (!seen_labels.insert(label).second) {
			throw BinderException("ATTACH (TYPE loki): duplicate label '%s' in the `labels` option", label);
		}
	}
	config.labels_pinned = labels_explicit;

	// A TTL governs re-discovery, so it is meaningless — and most likely a misunderstanding —
	// alongside an explicit `labels` list, which pins the columns for good.
	config.label_cache_ttl_ns = ParseTtlOption(info.options, "label_cache_ttl");
	if (labels_explicit && config.label_cache_ttl_ns != 0) {
		throw BinderException("ATTACH (TYPE loki): `label_cache_ttl` applies to auto-discovered label columns, but "
		                      "`labels` was given, which pins them; drop one of the two options");
	}

	// How many series the `series` table may materialize before giving up (DESIGN.md §3.6).
	auto series_cap = info.options.find("series_max_rows");
	if (series_cap != info.options.end() && !series_cap->second.IsNull()) {
		int64_t rows = 0;
		bool valid = series_cap->second.type().IsIntegral();
		if (valid) {
			rows = series_cap->second.GetValue<int64_t>();
			valid = rows > 0;
		}
		if (!valid) {
			throw BinderException("ATTACH (TYPE loki): `series_max_rows` must be a positive integer, got '%s'",
			                      series_cap->second.ToString());
		}
		config.series_max_rows = static_cast<idx_t>(rows);
	}

	// Resolve this tenant's promoted columns: the explicit list, or auto-discovered names.
	auto resolve_labels = [&](const string &tenant_id) -> vector<string> {
		if (labels_explicit) {
			return explicit_labels;
		}
		loki::AuthConfig tenant_auth = config.auth;
		if (!tenant_id.empty()) {
			tenant_auth.tenant = tenant_id;
		}
		return FetchLokiLabelNames(config.endpoint, tenant_auth);
	};

	// Tenants (DESIGN.md §3.6): one schema per `tenants` entry, else a single `public` schema
	// carrying whatever tenant the secret/`tenant` option resolved.
	auto tenant_ids = ParseStringListOption(info.options, "tenants");
	if (tenant_ids.empty()) {
		LokiTenant tenant;
		tenant.schema_name = "public";
		tenant.tenant_id = config.auth.tenant;
		tenant.label_columns = resolve_labels(tenant.tenant_id);
		config.tenants.push_back(std::move(tenant));
	} else {
		for (auto &tid : tenant_ids) {
			LokiTenant tenant;
			tenant.schema_name = tid;
			tenant.tenant_id = tid;
			tenant.label_columns = resolve_labels(tid);
			config.tenants.push_back(std::move(tenant));
		}
	}

	return make_uniq_base<Catalog, LokiCatalog>(db, std::move(config));
}

Transaction &LokiTransactionManager::StartTransaction(ClientContext &context) {
	// Re-fetch the label set if its TTL has elapsed, then pin the resulting generation for this
	// transaction's lifetime (DESIGN.md §11.8). This runs exactly once per (MetaTransaction,
	// attached database), so a statement touching `logs` and `series`, or the same table twice,
	// can never straddle two generations.
	catalog.MaybeRefreshLabels(context);
	auto transaction = make_uniq<LokiTransaction>(*this, context, catalog.PinCurrentSnapshot());
	auto &result = *transaction;
	lock_guard<mutex> guard(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}
ErrorData LokiTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	lock_guard<mutex> guard(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}
void LokiTransactionManager::RollbackTransaction(Transaction &transaction) {
	lock_guard<mutex> guard(transaction_lock);
	transactions.erase(transaction);
}
void LokiTransactionManager::Checkpoint(ClientContext &context, bool force) {
	// Read-only: nothing to persist.
}

unique_ptr<TransactionManager> LokiCreateTransactionManager(optional_ptr<StorageExtensionInfo>, AttachedDatabase &db,
                                                            Catalog &catalog) {
	// Read-only catalog: its transactions carry no state beyond the pinned schema generation
	// (DuckTransactionManager can't be reused — it asserts a DuckCatalog).
	return make_uniq<LokiTransactionManager>(db, catalog.Cast<LokiCatalog>());
}

//===--------------------------------------------------------------------===//
// PRAGMA loki_refresh_labels — force a label re-discovery (DESIGN.md §11.8)
//===--------------------------------------------------------------------===//

// Refresh one attached database, having verified it really is a loki catalog. Taking the
// transaction first is not decorative: it routes through MetaTransaction::UseDatabase, which holds
// a shared_ptr to the AttachedDatabase, keeping the catalog alive against a concurrent DETACH.
void RefreshOneDatabase(ClientContext &context, AttachedDatabase &db, bool skip_pinned) {
	Transaction::Get(context, db);
	auto &catalog = db.GetCatalog();
	if (catalog.GetCatalogType() != "loki") {
		throw InvalidInputException("PRAGMA loki_refresh_labels: database '%s' is not an ATTACHed Loki database "
		                            "(TYPE loki)",
		                            db.GetName());
	}
	auto &loki_catalog = catalog.Cast<LokiCatalog>();
	if (skip_pinned && loki_catalog.LabelsPinned()) {
		return; // nothing to discover; only an explicitly named database complains about this
	}
	loki_catalog.RefreshLabels(context);
}

void LokiRefreshLabelsPragma(ClientContext &context, const FunctionParameters &parameters) {
	auto &database_manager = DatabaseManager::Get(context);
	if (parameters.values.empty()) {
		// Bare `PRAGMA loki_refresh_labels`: every attached loki database, skipping the ones whose
		// labels are pinned rather than making the whole statement fail over them.
		for (auto &db : database_manager.GetDatabases(context)) {
			if (!db || db->GetCatalog().GetCatalogType() != "loki") {
				continue;
			}
			RefreshOneDatabase(context, *db, /*skip_pinned=*/true);
		}
		return;
	}
	auto name = parameters.values[0].ToString();
	auto db = database_manager.GetDatabase(context, Identifier(name));
	if (!db) {
		throw CatalogException("PRAGMA loki_refresh_labels: no attached database named '%s'", name);
	}
	RefreshOneDatabase(context, *db, /*skip_pinned=*/false);
}

void RegisterLokiStorageExtension(ExtensionLoader &loader) {
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	auto extension = make_shared_ptr<StorageExtension>();
	extension->attach = LokiAttach;
	extension->create_transaction_manager = LokiCreateTransactionManager;
	StorageExtension::Register(config, "loki", std::move(extension));

	// `PRAGMA loki_refresh_labels` refreshes every attached loki database;
	// `PRAGMA loki_refresh_labels('lk')` refreshes just that one and reports why it cannot.
	PragmaFunctionSet refresh_labels("loki_refresh_labels");
	refresh_labels.AddFunction(PragmaFunction::PragmaCall("loki_refresh_labels", LokiRefreshLabelsPragma, {}));
	refresh_labels.AddFunction(
	    PragmaFunction::PragmaCall("loki_refresh_labels", LokiRefreshLabelsPragma, {LogicalType::VARCHAR}));
	loader.RegisterFunction(std::move(refresh_labels));
}

} // namespace duckdb
