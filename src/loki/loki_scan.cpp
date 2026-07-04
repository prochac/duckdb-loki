#include "loki/loki_scan.hpp"

#include "loki/auth.hpp"
#include "loki/http_client.hpp"
#include "loki/http_request.hpp"
#include "loki/logql.hpp"
#include "loki/pager.hpp"
#include "loki/parse.hpp"
#include "loki/time_bounds.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include <string>
#include <vector>

namespace duckdb {

namespace {

constexpr int64_t NANOS_PER_HOUR = 3600LL * 1000LL * 1000LL * 1000LL;
constexpr int64_t DEFAULT_LIMIT = 5000;

struct LokiScanBindData : public TableFunctionData {
	std::string endpoint;
	std::string query;
	int64_t start_ns = 0;
	int64_t end_ns = 0;
	int64_t limit = DEFAULT_LIMIT;
	std::string direction = "backward";
	loki::AuthConfig auth;
	std::vector<std::string> label_columns; // labels promoted to top-level columns, in declared order
};

struct LokiScanGlobalState : public GlobalTableFunctionState {
	LokiScanGlobalState(std::string endpoint_p, std::vector<std::pair<std::string, std::string>> headers_p,
	                    loki::StreamPager pager_p)
	    : endpoint(std::move(endpoint_p)), headers(std::move(headers_p)), pager(std::move(pager_p)) {
	}

	std::string endpoint;
	std::vector<std::pair<std::string, std::string>> headers;
	loki::StreamPager pager;
	std::vector<loki::LokiRow> buffer;
	idx_t buf_offset = 0;
	std::vector<column_t> column_ids; // projected bind-schema columns to emit, in output order

	idx_t MaxThreads() const override {
		return 1;
	}
};

// Convert a bound TIMESTAMP/TIMESTAMPTZ/VARCHAR parameter to a nanosecond Unix epoch.
// Casting to TIMESTAMP yields UTC microseconds, which GetEpochNanoSeconds scales to ns.
int64_t ValueToEpochNs(const Value &value) {
	Value as_ts = value.DefaultCastAs(LogicalType::TIMESTAMP);
	auto ts = as_ts.GetValue<timestamp_t>();
	return Timestamp::GetEpochNanoSeconds(ts);
}

// Resolve a start/end bound. An INTERVAL is an offset added to now() (so `-INTERVAL 2 HOUR`
// means two hours ago); any timestamp-like value is taken as an absolute instant.
int64_t ResolveTimeBound(const Value &value, int64_t now_ns) {
	if (value.type().id() == LogicalTypeId::INTERVAL) {
		auto iv = value.GetValue<interval_t>();
		try {
			return now_ns + loki::IntervalToNanos(iv.months, iv.days, iv.micros);
		} catch (const std::exception &e) {
			throw BinderException(e.what());
		}
	}
	return ValueToEpochNs(value);
}

// Extract key/value pairs from a MAP(VARCHAR, VARCHAR) Value into a header list.
void AppendHeadersFromMap(const Value &map_value, std::vector<std::pair<std::string, std::string>> &out) {
	if (map_value.IsNull()) {
		return;
	}
	for (const auto &entry : ListValue::GetChildren(map_value)) {
		auto kv = StructValue::GetChildren(entry);
		out.emplace_back(kv[0].ToString(), kv[1].ToString());
	}
}

// Look up a named parameter, returning nullptr when absent or explicitly NULL.
const Value *FindParam(TableFunctionBindInput &input, const char *name) {
	auto it = input.named_parameters.find(name);
	if (it == input.named_parameters.end() || it->second.IsNull()) {
		return nullptr;
	}
	return &it->second;
}

// Copy a `loki` secret's fields into the connection config being assembled.
void ReadLokiSecret(const KeyValueSecret &kv, std::string &endpoint, loki::AuthConfig &auth,
                    std::vector<std::pair<std::string, std::string>> &headers) {
	Value v;
	if (kv.TryGetValue("endpoint", v) && !v.IsNull()) {
		endpoint = v.ToString();
	}
	if (kv.TryGetValue("token", v) && !v.IsNull()) {
		auth.token = v.ToString();
	}
	if (kv.TryGetValue("username", v) && !v.IsNull()) {
		auth.username = v.ToString();
	}
	if (kv.TryGetValue("password", v) && !v.IsNull()) {
		auth.password = v.ToString();
	}
	if (kv.TryGetValue("tenant", v) && !v.IsNull()) {
		auth.tenant = v.ToString();
	}
	if (kv.TryGetValue("headers", v)) {
		AppendHeadersFromMap(v, headers);
	}
}

unique_ptr<FunctionData> LokiScanBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<LokiScanBindData>();

	if (input.inputs.empty() || input.inputs[0].IsNull()) {
		throw BinderException("loki_scan requires a LogQL query string as its first argument");
	}
	result->query = loki::BuildLogQL(input.inputs[0].GetValue<string>());

	// --- Resolve connection config: a `loki` secret, refined by inline overrides. ---
	std::string endpoint;
	std::vector<std::pair<std::string, std::string>> secret_headers;

	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	if (auto *secret_param = FindParam(input, "secret")) {
		// Explicit secret name — must exist.
		auto name = secret_param->ToString();
		auto entry = secret_manager.GetSecretByName(transaction, name);
		if (!entry) {
			throw BinderException("loki_scan: no secret named '%s' found", name);
		}
		ReadLokiSecret(dynamic_cast<const KeyValueSecret &>(*entry->secret), endpoint, result->auth, secret_headers);
	} else if (auto entry = secret_manager.GetSecretByName(transaction, "loki")) {
		// No explicit secret: fall back to a default secret named "loki" if one exists.
		ReadLokiSecret(dynamic_cast<const KeyValueSecret &>(*entry->secret), endpoint, result->auth, secret_headers);
	}

	// Inline overrides take precedence over the secret.
	if (auto *p = FindParam(input, "endpoint")) {
		endpoint = p->ToString();
	}
	if (auto *p = FindParam(input, "token")) {
		result->auth.token = p->ToString();
	}
	if (auto *p = FindParam(input, "username")) {
		result->auth.username = p->ToString();
	}
	if (auto *p = FindParam(input, "password")) {
		result->auth.password = p->ToString();
	}
	if (auto *p = FindParam(input, "tenant")) {
		result->auth.tenant = p->ToString();
	}
	if (auto *p = FindParam(input, "headers")) {
		// Inline headers replace the secret's headers.
		secret_headers.clear();
		AppendHeadersFromMap(*p, secret_headers);
	}
	result->auth.extra_headers = std::move(secret_headers);

	if (endpoint.empty()) {
		throw BinderException("loki_scan requires an endpoint: pass endpoint := 'http://localhost:3100' or "
		                      "reference a secret (secret := 'my_loki', or a default secret named 'loki') "
		                      "created with CREATE SECRET (TYPE loki, ENDPOINT ...)");
	}
	while (!endpoint.empty() && endpoint.back() == '/') {
		endpoint.pop_back();
	}
	result->endpoint = std::move(endpoint);

	if (auto *p = FindParam(input, "limit")) {
		result->limit = p->GetValue<int64_t>();
		if (result->limit <= 0) {
			throw BinderException("loki_scan 'limit' must be a positive integer");
		}
	}

	if (auto *p = FindParam(input, "direction")) {
		result->direction = StringUtil::Lower(p->ToString());
		if (result->direction != "backward" && result->direction != "forward") {
			throw BinderException("loki_scan 'direction' must be 'backward' or 'forward'");
		}
	}

	// Default to the last hour, ending now (DESIGN.md §6.6) — never issue an unbounded query.
	int64_t now_ns = Timestamp::GetEpochNanoSeconds(Timestamp::GetCurrentTimestamp());
	result->end_ns = now_ns;
	result->start_ns = now_ns - NANOS_PER_HOUR;
	if (auto *p = FindParam(input, "start")) {
		result->start_ns = ResolveTimeBound(*p, now_ns);
	}
	if (auto *p = FindParam(input, "end")) {
		result->end_ns = ResolveTimeBound(*p, now_ns);
	}

	// `labels := ['job','level']` promotes each named label to a top-level VARCHAR column so
	// its values are directly selectable (and, from v0.4, pushable). Unlisted labels still
	// appear inside the `labels` MAP. See DESIGN.md §3.2 / §4.2.
	if (auto *p = FindParam(input, "labels")) {
		for (const auto &child : ListValue::GetChildren(*p)) {
			if (child.IsNull()) {
				throw BinderException("loki_scan 'labels' must not contain NULL entries");
			}
			result->label_columns.push_back(child.ToString());
		}
	}

	// Output schema (DESIGN.md §3.2): timestamp, line, one column per promoted label,
	// then the full labels MAP and per-entry structured_metadata MAP.
	return_types.push_back(LogicalType::TIMESTAMP_NS);
	names.emplace_back("timestamp");
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("line");
	for (const auto &label : result->label_columns) {
		return_types.push_back(LogicalType::VARCHAR);
		names.push_back(label);
	}
	return_types.push_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
	names.emplace_back("labels");
	return_types.push_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
	names.emplace_back("structured_metadata");

	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> LokiScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<LokiScanBindData>();

	auto headers = loki::BuildAuthHeaders(bind_data.auth);
	// Request categorized labels so structured metadata comes back per-entry (separated from the
	// indexed stream labels) instead of being folded into the stream label set. Servers that do
	// not support the flag ignore it and simply return no structured metadata (DESIGN.md §5).
	headers.emplace_back("X-Loki-Response-Encoding-Flags", "categorize-labels");
	loki::StreamPager pager(bind_data.query, bind_data.start_ns, bind_data.end_ns, bind_data.direction,
	                        bind_data.limit);

	// No network here: the first page is fetched lazily on the first output call so that
	// binding/DESCRIBE stay cheap and offline.
	auto state = make_uniq<LokiScanGlobalState>(bind_data.endpoint, std::move(headers), std::move(pager));
	// Projection pushdown (DESIGN.md §6.3): column_ids holds the bind-schema indices to emit,
	// in output order, so we only build the requested columns (skipping the expensive MAPs).
	state->column_ids = input.column_ids;
	return std::move(state);
}

// Build a MAP(VARCHAR, VARCHAR) Value from a string map (labels or structured metadata).
Value MapFromStrings(const std::map<std::string, std::string> &m) {
	InsertionOrderPreservingMap<string> ordered;
	for (const auto &kv : m) {
		ordered.insert(kv.first, kv.second);
	}
	return Value::MAP(ordered);
}

void LokiScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<LokiScanGlobalState>();
	auto &bind_data = data_p.bind_data->Cast<LokiScanBindData>();

	// Source-column index meaning: 0=timestamp, 1=line, [2, 2+N)=promoted label column,
	// 2+N=labels MAP, 3+N=structured_metadata MAP (N = number of promoted labels).
	const idx_t num_labels = bind_data.label_columns.size();
	const column_t labels_map_col = 2 + num_labels;
	const column_t smeta_map_col = 3 + num_labels;

	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE) {
		if (state.buf_offset >= state.buffer.size()) {
			if (state.pager.Done()) {
				break;
			}
			// Fetch and decode the next page.
			auto request = state.pager.NextRequest();
			std::string query_string = loki::BuildQueryString(request);
			auto response = loki::HttpGet(state.endpoint, request.path, query_string, state.headers);
			if (!response.error.empty()) {
				throw IOException("loki_scan: request to Loki failed: " + response.error);
			}
			if (response.status < 200 || response.status >= 300) {
				throw IOException(StringUtil::Format("loki_scan: Loki returned HTTP %d: %s", response.status,
				                                     response.body.substr(0, 1000)));
			}
			// ParseStreamsResponse throws std::runtime_error on malformed/non-streams bodies.
			auto chunks = loki::ParseStreamsResponse(response.body);
			state.buffer = state.pager.Accept(chunks);
			state.buf_offset = 0;
			if (state.buffer.empty()) {
				continue; // page yielded no new rows (all de-duped, or exhausted); loop re-checks Done()
			}
		}

		const auto &row = state.buffer[state.buf_offset];
		// Projection pushdown: emit only the requested columns, in output order. output.data[out]
		// corresponds to source column state.column_ids[out]. Unrequested MAP columns are skipped.
		for (idx_t out = 0; out < state.column_ids.size(); out++) {
			const column_t src = state.column_ids[out];
			if (src == COLUMN_IDENTIFIER_ROW_ID || IsVirtualColumn(src)) {
				continue; // we emit no row-ids or virtual columns
			}
			auto &vec = output.data[out];
			if (src == 0) {
				// TIMESTAMP_NS is stored as raw nanoseconds (do NOT use Timestamp::FromEpochNanoSeconds,
				// which rescales to microseconds for the µs TIMESTAMP type).
				FlatVector::GetData<int64_t>(vec)[count] = row.ts_ns;
			} else if (src == 1) {
				vec.SetValue(count, Value(row.line));
			} else if (src < labels_map_col) {
				// A promoted label column: its value, or NULL where the stream lacks the label.
				const auto &name = bind_data.label_columns[src - 2];
				auto it = row.labels.find(name);
				vec.SetValue(count, it != row.labels.end() ? Value(it->second) : Value(LogicalType::VARCHAR));
			} else if (src == labels_map_col) {
				vec.SetValue(count, MapFromStrings(row.labels));
			} else { // smeta_map_col
				vec.SetValue(count, MapFromStrings(row.structured_metadata));
			}
		}

		state.buf_offset++;
		count++;
	}

	output.SetCardinality(count);
}

} // namespace

void RegisterLokiScanFunction(ExtensionLoader &loader) {
	TableFunction loki_scan("loki_scan", {LogicalType::VARCHAR}, LokiScanFunction, LokiScanBind, LokiScanInitGlobal);
	loki_scan.named_parameters["endpoint"] = LogicalType::VARCHAR;
	loki_scan.named_parameters["secret"] = LogicalType::VARCHAR;
	loki_scan.named_parameters["token"] = LogicalType::VARCHAR;
	loki_scan.named_parameters["username"] = LogicalType::VARCHAR;
	loki_scan.named_parameters["password"] = LogicalType::VARCHAR;
	loki_scan.named_parameters["tenant"] = LogicalType::VARCHAR;
	loki_scan.named_parameters["headers"] = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	// start/end accept TIMESTAMP, TIMESTAMPTZ, or INTERVAL (offset from now); typed ANY and
	// dispatched at bind time.
	loki_scan.named_parameters["start"] = LogicalType::ANY;
	loki_scan.named_parameters["end"] = LogicalType::ANY;
	loki_scan.named_parameters["limit"] = LogicalType::BIGINT;
	loki_scan.named_parameters["direction"] = LogicalType::VARCHAR;
	// labels := ['job','level'] promotes labels to top-level VARCHAR columns (DESIGN.md §3.2).
	loki_scan.named_parameters["labels"] = LogicalType::LIST(LogicalType::VARCHAR);
	// Projection pushdown (DESIGN.md §6.3): only build the columns the query selects. We do NOT
	// set filter_pushdown/filter_prune — those are v0.4.
	loki_scan.projection_pushdown = true;
	loader.RegisterFunction(loki_scan);
}

} // namespace duckdb
