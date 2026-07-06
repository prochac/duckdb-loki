#include "loki/catalog.hpp"

#include "loki/connection.hpp"
#include "loki/http_client.hpp"
#include "loki/http_request.hpp"
#include "loki/logql.hpp"
#include "loki/loki_discovery.hpp"
#include "loki/loki_scan.hpp"
#include "loki/parse.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

#include <utility>

namespace duckdb {

namespace {

constexpr int64_t NANOS_PER_HOUR = 3600LL * 1000LL * 1000LL * 1000LL;

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

// Auto-discover the promoted label columns for a tenant by calling GET /labels at attach time
// (DESIGN.md §3.6). Used only when the ATTACH `labels` option is absent — an explicit `labels`
// (even an empty list) opts out. Errors surface clearly so an unreachable Loki fails the ATTACH.
vector<string> FetchLokiLabelNames(const std::string &endpoint, const loki::AuthConfig &auth) {
	auto request = loki::BuildLabelsRequest(-1, -1); // default window: let Loki decide
	auto headers = loki::BuildAuthHeaders(auth);
	auto query_string = loki::BuildQueryString(request);
	auto response = loki::HttpGet(endpoint, request.path, query_string, headers);
	if (!response.error.empty()) {
		throw IOException("ATTACH (TYPE loki): label auto-discovery request to Loki failed: " + response.error);
	}
	if (response.status < 200 || response.status >= 300) {
		throw IOException(StringUtil::Format("ATTACH (TYPE loki): label auto-discovery got HTTP %d: %s",
		                                     response.status, response.body.substr(0, 1000)));
	}
	try {
		return loki::ParseStringArrayResponse(response.body);
	} catch (const std::exception &e) {
		throw IOException(std::string("ATTACH (TYPE loki): label auto-discovery: ") + e.what());
	}
}

// Build a MAP(VARCHAR, VARCHAR) Value from a label set (same as the logs/discovery scans).
Value MapFromStrings(const std::map<std::string, std::string> &m) {
	InsertionOrderPreservingMap<string> ordered;
	for (const auto &kv : m) {
		ordered.insert(kv.first, kv.second);
	}
	return Value::MAP(ordered);
}

// Add the `series` table's columns: one VARCHAR per promoted label, then the full labels MAP.
void BuildLokiSeriesColumns(const std::vector<std::string> &label_columns, ColumnList &columns) {
	for (const auto &label : label_columns) {
		columns.AddColumn(ColumnDefinition(label, LogicalType::VARCHAR));
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
		throw BinderException("loki series: a stream selector is required — add a WHERE equality/IN on a promoted "
		                      "label column (Loki refuses /series without a selector)");
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
			throw IOException("loki series: request to Loki failed: " + response.error);
		}
		if (response.status < 200 || response.status >= 300) {
			throw IOException(StringUtil::Format("loki series: Loki returned HTTP %d: %s", response.status,
			                                     response.body.substr(0, 1000)));
		}
		try {
			state.rows = loki::ParseSeriesResponse(response.body);
		} catch (const std::exception &e) {
			throw IOException(std::string("loki series: ") + e.what());
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
				continue;
			}
			auto &vec = output.data[out];
			if (src < labels_map_col) {
				const auto &name = bind_data.label_columns[src];
				auto it = row.find(name);
				vec.SetValue(count, it != row.end() ? Value(it->second) : Value(LogicalType::VARCHAR));
			} else {
				vec.SetValue(count, MapFromStrings(row));
			}
		}
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

} // namespace

//===--------------------------------------------------------------------===//
// LokiCatalog
//===--------------------------------------------------------------------===//

LokiCatalog::LokiCatalog(AttachedDatabase &db, LokiCatalogConfig config_p) : Catalog(db), config(std::move(config_p)) {
}

LokiCatalog::~LokiCatalog() = default;

void LokiCatalog::Initialize(bool load_builtin) {
	// Build the schema tree eagerly: it is fully determined by the attach-time config (endpoint,
	// auth, per-tenant label columns), so no Loki round-trip happens here.
	for (const auto &tenant : config.tenants) {
		CreateSchemaInfo schema_info;
		schema_info.schema = tenant.schema_name;
		schemas.push_back(make_uniq<LokiSchemaEntry>(*this, schema_info, config, tenant));
	}
}

optional_ptr<SchemaCatalogEntry> LokiCatalog::LookupSchema(CatalogTransaction transaction,
                                                           const EntryLookupInfo &schema_lookup,
                                                           OnEntryNotFound if_not_found) {
	const auto &name = schema_lookup.GetEntryName();
	for (auto &schema : schemas) {
		if (StringUtil::CIEquals(schema->name, name)) {
			return schema.get();
		}
	}
	if (if_not_found == OnEntryNotFound::RETURN_NULL) {
		return nullptr;
	}
	throw CatalogException("Schema '%s' not found in loki catalog '%s'", name, GetName());
}

void LokiCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	for (auto &schema : schemas) {
		callback(*schema);
	}
}

string LokiCatalog::GetDefaultSchema() const {
	return config.tenants.empty() ? "public" : config.tenants.front().schema_name;
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

	CreateTableInfo logs_info(catalog.GetName(), tenant.schema_name, "logs");
	BuildLokiLogsColumns(tenant.label_columns, logs_info.columns);
	logs_table =
	    make_uniq<LokiLogsTable>(catalog, *this, logs_info, catalog_config.endpoint, auth, tenant.label_columns);

	// Optional exploration tables (DESIGN.md §3.6): `labels` (all label names) and `series`
	// (matching series, selector-driven like `logs`).
	CreateTableInfo labels_info(catalog.GetName(), tenant.schema_name, "labels");
	labels_info.columns.AddColumn(ColumnDefinition("label", LogicalType::VARCHAR));
	labels_table = make_uniq<LokiLabelsTable>(catalog, *this, labels_info, catalog_config.endpoint, auth);

	CreateTableInfo series_info(catalog.GetName(), tenant.schema_name, "series");
	BuildLokiSeriesColumns(tenant.label_columns, series_info.columns);
	series_table = make_uniq<LokiSeriesTable>(catalog, *this, series_info, catalog_config.endpoint, std::move(auth),
	                                          tenant.label_columns);
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
    : TableCatalogEntry(catalog, schema, info), endpoint(std::move(endpoint_p)), auth(std::move(auth_p)),
      label_columns(std::move(label_columns_p)) {
}

TableFunction LokiLogsTable::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto bd = make_uniq<LokiScanBindData>();
	bd->pushdown_mode = true; // WHERE synthesizes the selector; the mandatory-selector rule
	bd->base_selector = "";   // (DESIGN.md §4.3) is enforced at scan start, so a bare scan errors.
	bd->endpoint = endpoint;
	bd->auth = auth;
	bd->label_columns = label_columns;
	// The catalog path bypasses LokiResolveCommon, where the function path sets the default window;
	// set the last-hour default here (DESIGN.md §6.6) so a query without a timestamp WHERE is bounded.
	int64_t now_ns = Timestamp::GetEpochNanoSeconds(Timestamp::GetCurrentTimestamp());
	bd->end_ns = now_ns;
	bd->start_ns = now_ns - NANOS_PER_HOUR;
	bind_data = std::move(bd);
	return MakeLokiCatalogScanFunction();
}

//===--------------------------------------------------------------------===//
// LokiLabelsTable
//===--------------------------------------------------------------------===//

LokiLabelsTable::LokiLabelsTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                 std::string endpoint_p, loki::AuthConfig auth_p)
    : TableCatalogEntry(catalog, schema, info), endpoint(std::move(endpoint_p)), auth(std::move(auth_p)) {
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
                                 std::vector<std::string> label_columns_p)
    : TableCatalogEntry(catalog, schema, info), endpoint(std::move(endpoint_p)), auth(std::move(auth_p)),
      label_columns(std::move(label_columns_p)) {
}

TableFunction LokiSeriesTable::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto bd = make_uniq<LokiScanBindData>();
	bd->pushdown_mode = true; // WHERE label predicates synthesize the match[] selector.
	bd->base_selector = "";
	bd->endpoint = endpoint;
	bd->auth = auth;
	bd->label_columns = label_columns;
	int64_t now_ns = Timestamp::GetEpochNanoSeconds(Timestamp::GetCurrentTimestamp());
	bd->end_ns = now_ns;
	bd->start_ns = now_ns - NANOS_PER_HOUR;
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
	auto transaction = make_uniq<Transaction>(*this, context);
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
	// Read-only catalog: its transactions carry no state (DuckTransactionManager can't be reused —
	// it asserts a DuckCatalog).
	return make_uniq<LokiTransactionManager>(db);
}

void RegisterLokiStorageExtension(DBConfig &config) {
	auto extension = make_shared_ptr<StorageExtension>();
	extension->attach = LokiAttach;
	extension->create_transaction_manager = LokiCreateTransactionManager;
	StorageExtension::Register(config, "loki", std::move(extension));
}

} // namespace duckdb
