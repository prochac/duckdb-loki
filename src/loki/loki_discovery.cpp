#include "loki/loki_discovery.hpp"

#include "loki/auth.hpp"
#include "loki/connection.hpp"
#include "loki/http_client.hpp"
#include "loki/http_request.hpp"
#include "loki/parse.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table_function.hpp"

#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

namespace duckdb {

namespace {

struct LokiDiscoveryGlobalState : public GlobalTableFunctionState {
	LokiDiscoveryGlobalState(std::string endpoint_p, std::vector<std::pair<std::string, std::string>> headers_p,
	                         loki::QueryRangeRequest request_p)
	    : endpoint(std::move(endpoint_p)), headers(std::move(headers_p)), request(std::move(request_p)) {
	}

	std::string endpoint;
	std::vector<std::pair<std::string, std::string>> headers;
	loki::QueryRangeRequest request;

	bool fetched = false;
	std::vector<std::string> string_rows;                        // LABELS / LABEL_VALUES
	std::vector<std::map<std::string, std::string>> series_rows; // SERIES
	idx_t offset = 0;

	idx_t MaxThreads() const override {
		return 1;
	}
};

// Resolve the optional start/end time window. Discovery endpoints are inherently bounded by
// Loki's own default window when unset, so an omitted bound stays a negative sentinel and is
// simply not sent — letting Loki decide, while power users can widen it explicitly.
void ResolveDiscoveryBounds(TableFunctionBindInput &input, int64_t &start_ns, int64_t &end_ns) {
	start_ns = -1;
	end_ns = -1;
	int64_t now_ns = Timestamp::GetEpochNanoSeconds(Timestamp::GetCurrentTimestamp());
	if (auto *p = FindNamedParam(input, "start")) {
		start_ns = ResolveTimeBound(*p, now_ns);
	}
	if (auto *p = FindNamedParam(input, "end")) {
		end_ns = ResolveTimeBound(*p, now_ns);
	}
}

unique_ptr<FunctionData> LokiLabelsBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<LokiDiscoveryBindData>();
	result->kind = DiscoveryKind::LABELS;
	ResolveLokiConnection(context, input, result->endpoint, result->auth, "loki_labels");

	int64_t start_ns, end_ns;
	ResolveDiscoveryBounds(input, start_ns, end_ns);
	result->request = loki::BuildLabelsRequest(start_ns, end_ns);

	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("label");
	return std::move(result);
}

unique_ptr<FunctionData> LokiLabelValuesBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<LokiDiscoveryBindData>();
	result->kind = DiscoveryKind::LABEL_VALUES;
	ResolveLokiConnection(context, input, result->endpoint, result->auth, "loki_label_values");

	if (input.inputs.empty() || input.inputs[0].IsNull()) {
		throw BinderException("loki_label_values requires a label name as its first argument");
	}
	std::string label = input.inputs[0].GetValue<string>();

	int64_t start_ns, end_ns;
	ResolveDiscoveryBounds(input, start_ns, end_ns);
	// An optional `query` selector narrows which series contribute values (DESIGN.md §5).
	std::string query;
	if (auto *p = FindNamedParam(input, "query")) {
		query = p->ToString();
	}
	result->request = loki::BuildLabelValuesRequest(label, start_ns, end_ns, query);

	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("value");
	return std::move(result);
}

unique_ptr<FunctionData> LokiSeriesBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<LokiDiscoveryBindData>();
	result->kind = DiscoveryKind::SERIES;
	ResolveLokiConnection(context, input, result->endpoint, result->auth, "loki_series");

	// Loki rejects /series without a selector, so require the match argument (DESIGN.md §3.4).
	if (input.inputs.empty() || input.inputs[0].IsNull()) {
		throw BinderException("loki_series requires a stream selector as its first argument, e.g. "
		                      "loki_series('{job=\"api\"}')");
	}
	std::string match = input.inputs[0].GetValue<string>();

	int64_t start_ns, end_ns;
	ResolveDiscoveryBounds(input, start_ns, end_ns);
	result->request = loki::BuildSeriesRequest(match, start_ns, end_ns);

	return_types.push_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
	names.emplace_back("labels");
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> LokiDiscoveryInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<LokiDiscoveryBindData>();
	auto headers = loki::BuildAuthHeaders(bind_data.auth);
	// No network here: the request fires lazily on the first output call so binding stays offline.
	return make_uniq<LokiDiscoveryGlobalState>(bind_data.endpoint, std::move(headers), bind_data.request);
}

// Build a MAP(VARCHAR, VARCHAR) Value from a label set.
Value MapFromStrings(const std::map<std::string, std::string> &m) {
	InsertionOrderPreservingMap<string> ordered;
	for (const auto &kv : m) {
		ordered.insert(kv.first, kv.second);
	}
	return Value::MAP(ordered);
}

// Issue the single discovery request and parse it into the appropriate buffer. Errors surface the
// Loki status and body, matching the scan path.
void FetchDiscovery(LokiDiscoveryGlobalState &state, DiscoveryKind kind, const char *fn) {
	std::string query_string = loki::BuildQueryString(state.request);
	auto response = loki::HttpGet(state.endpoint, state.request.path, query_string, state.headers);
	if (!response.error.empty()) {
		throw IOException(StringUtil::Format("%s: request to Loki failed: %s", fn, response.error));
	}
	if (response.status < 200 || response.status >= 300) {
		throw IOException(
		    StringUtil::Format("%s: Loki returned HTTP %d: %s", fn, response.status, response.body.substr(0, 1000)));
	}
	// The parsers throw std::runtime_error on malformed bodies; surface as an IOException.
	try {
		if (kind == DiscoveryKind::SERIES) {
			state.series_rows = loki::ParseSeriesResponse(response.body);
		} else {
			state.string_rows = loki::ParseStringArrayResponse(response.body);
		}
	} catch (const std::exception &e) {
		throw IOException(StringUtil::Format("%s: %s", fn, e.what()));
	}
}

void LokiDiscoveryFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<LokiDiscoveryGlobalState>();
	auto &bind_data = data_p.bind_data->Cast<LokiDiscoveryBindData>();

	if (!state.fetched) {
		const char *fn = bind_data.kind == DiscoveryKind::LABELS         ? "loki_labels"
		                 : bind_data.kind == DiscoveryKind::LABEL_VALUES ? "loki_label_values"
		                                                                 : "loki_series";
		FetchDiscovery(state, bind_data.kind, fn);
		state.fetched = true;
	}

	idx_t count = 0;
	if (bind_data.kind == DiscoveryKind::SERIES) {
		while (count < STANDARD_VECTOR_SIZE && state.offset < state.series_rows.size()) {
			output.data[0].SetValue(count, MapFromStrings(state.series_rows[state.offset]));
			state.offset++;
			count++;
		}
	} else {
		while (count < STANDARD_VECTOR_SIZE && state.offset < state.string_rows.size()) {
			output.data[0].SetValue(count, Value(state.string_rows[state.offset]));
			state.offset++;
			count++;
		}
	}
	output.SetCardinality(count);
}

// Connection/auth + optional time-window named parameters shared by all discovery functions.
void AddDiscoveryNamedParameters(TableFunction &fn) {
	fn.named_parameters["endpoint"] = LogicalType::VARCHAR;
	fn.named_parameters["secret"] = LogicalType::VARCHAR;
	fn.named_parameters["token"] = LogicalType::VARCHAR;
	fn.named_parameters["username"] = LogicalType::VARCHAR;
	fn.named_parameters["password"] = LogicalType::VARCHAR;
	fn.named_parameters["tenant"] = LogicalType::VARCHAR;
	fn.named_parameters["headers"] = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	// start/end accept TIMESTAMP, TIMESTAMPTZ, or INTERVAL (offset from now); omitted → Loki default.
	fn.named_parameters["start"] = LogicalType::ANY;
	fn.named_parameters["end"] = LogicalType::ANY;
}

} // namespace

// Bind-less discovery scan for the ATTACH catalog's `labels` table (DESIGN.md §3.6): the entry
// supplies a pre-built LokiDiscoveryBindData; DuckDB uses this function's execute + init only.
TableFunction MakeLokiDiscoveryScanFunction() {
	return TableFunction("loki_discovery", {}, LokiDiscoveryFunction, /*bind=*/nullptr, LokiDiscoveryInitGlobal);
}

void RegisterLokiDiscoveryFunctions(ExtensionLoader &loader) {
	// loki_labels(...) — GET /labels, one row per label name (DESIGN.md §3.4).
	TableFunction labels("loki_labels", {}, LokiDiscoveryFunction, LokiLabelsBind, LokiDiscoveryInitGlobal);
	AddDiscoveryNamedParameters(labels);
	loader.RegisterFunction(labels);

	// loki_label_values(name, ...) — GET /label/{name}/values, one row per value.
	TableFunction label_values("loki_label_values", {LogicalType::VARCHAR}, LokiDiscoveryFunction, LokiLabelValuesBind,
	                           LokiDiscoveryInitGlobal);
	AddDiscoveryNamedParameters(label_values);
	// A selector to restrict which series contribute values.
	label_values.named_parameters["query"] = LogicalType::VARCHAR;
	loader.RegisterFunction(label_values);

	// loki_series(match, ...) — GET /series, one MAP row per matching series.
	TableFunction series("loki_series", {LogicalType::VARCHAR}, LokiDiscoveryFunction, LokiSeriesBind,
	                     LokiDiscoveryInitGlobal);
	AddDiscoveryNamedParameters(series);
	loader.RegisterFunction(series);
}

} // namespace duckdb
