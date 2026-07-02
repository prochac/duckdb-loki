#include "loki/loki_scan.hpp"

#include "loki/http_client.hpp"
#include "loki/http_request.hpp"
#include "loki/logql.hpp"
#include "loki/parse.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table_function.hpp"

#include <map>
#include <string>
#include <vector>

namespace duckdb {

namespace {

constexpr int64_t NANOS_PER_HOUR = 3600LL * 1000LL * 1000LL * 1000LL;
constexpr int64_t DEFAULT_LIMIT = 5000;

// One flattened output row: a stream entry paired with its stream's labels.
struct LokiRow {
	int64_t ts_ns;
	std::string line;
	std::map<std::string, std::string> labels;
};

struct LokiScanBindData : public TableFunctionData {
	std::string endpoint;
	std::string query;
	int64_t start_ns = 0;
	int64_t end_ns = 0;
	int64_t limit = DEFAULT_LIMIT;
	std::string direction = "backward";
};

struct LokiScanGlobalState : public GlobalTableFunctionState {
	std::vector<LokiRow> rows;
	idx_t offset = 0;

	idx_t MaxThreads() const override {
		return 1;
	}
};

// Convert a bound TIMESTAMP/TIMESTAMPTZ/VARCHAR parameter to a nanosecond Unix epoch.
// The param is declared TIMESTAMP_TZ, so casting to TIMESTAMP yields UTC microseconds,
// which GetEpochNanoSeconds scales to nanoseconds.
int64_t ValueToEpochNs(const Value &value) {
	Value as_ts = value.DefaultCastAs(LogicalType::TIMESTAMP);
	auto ts = as_ts.GetValue<timestamp_t>();
	return Timestamp::GetEpochNanoSeconds(ts);
}

unique_ptr<FunctionData> LokiScanBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<LokiScanBindData>();

	if (input.inputs.empty() || input.inputs[0].IsNull()) {
		throw BinderException("loki_scan requires a LogQL query string as its first argument");
	}
	result->query = loki::BuildLogQL(input.inputs[0].GetValue<string>());

	// endpoint is mandatory in v0.1 (secret-based config arrives in a later version).
	auto endpoint_entry = input.named_parameters.find("endpoint");
	if (endpoint_entry == input.named_parameters.end() || endpoint_entry->second.IsNull()) {
		throw BinderException("loki_scan requires an 'endpoint' parameter, e.g. "
		                      "endpoint := 'http://localhost:3100'");
	}
	result->endpoint = endpoint_entry->second.ToString();
	while (!result->endpoint.empty() && result->endpoint.back() == '/') {
		result->endpoint.pop_back();
	}

	auto limit_entry = input.named_parameters.find("limit");
	if (limit_entry != input.named_parameters.end() && !limit_entry->second.IsNull()) {
		result->limit = limit_entry->second.GetValue<int64_t>();
		if (result->limit <= 0) {
			throw BinderException("loki_scan 'limit' must be a positive integer");
		}
	}

	// Default to the last hour, ending now (DESIGN.md §6.6) — never issue an unbounded query.
	int64_t now_ns = Timestamp::GetEpochNanoSeconds(Timestamp::GetCurrentTimestamp());
	result->end_ns = now_ns;
	result->start_ns = now_ns - NANOS_PER_HOUR;

	auto start_entry = input.named_parameters.find("start");
	if (start_entry != input.named_parameters.end() && !start_entry->second.IsNull()) {
		result->start_ns = ValueToEpochNs(start_entry->second);
	}
	auto end_entry = input.named_parameters.find("end");
	if (end_entry != input.named_parameters.end() && !end_entry->second.IsNull()) {
		result->end_ns = ValueToEpochNs(end_entry->second);
	}

	return_types.push_back(LogicalType::TIMESTAMP_NS);
	names.emplace_back("timestamp");
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("line");
	return_types.push_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
	names.emplace_back("labels");

	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> LokiScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<LokiScanBindData>();
	auto state = make_uniq<LokiScanGlobalState>();

	auto request =
	    loki::BuildQueryRangeRequest(bind_data.query, bind_data.start_ns, bind_data.end_ns, bind_data.limit,
	                                 bind_data.direction);
	std::string query_string = loki::BuildQueryString(request);

	std::vector<std::pair<std::string, std::string>> headers; // v0.1: no auth headers yet
	auto response = loki::HttpGet(bind_data.endpoint, request.path, query_string, headers);
	if (!response.error.empty()) {
		throw IOException("loki_scan: request to Loki failed: " + response.error);
	}
	if (response.status < 200 || response.status >= 300) {
		throw IOException(StringUtil::Format("loki_scan: Loki returned HTTP %d: %s", response.status,
		                                     response.body.substr(0, 1000)));
	}

	// ParseStreamsResponse throws std::runtime_error on malformed/non-streams bodies;
	// that propagates to the user as a query error.
	auto chunks = loki::ParseStreamsResponse(response.body);
	for (auto &chunk : chunks) {
		for (auto &entry : chunk.values) {
			LokiRow row;
			row.ts_ns = entry.first;
			row.line = std::move(entry.second);
			row.labels = chunk.labels;
			state->rows.push_back(std::move(row));
		}
	}

	return std::move(state);
}

void LokiScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<LokiScanGlobalState>();

	// TIMESTAMP_NS is stored as raw nanoseconds (do NOT use Timestamp::FromEpochNanoSeconds,
	// which rescales to microseconds for the µs TIMESTAMP type).
	auto timestamps = FlatVector::GetData<int64_t>(output.data[0]);

	idx_t count = 0;
	const idx_t total = state.rows.size();
	while (state.offset < total && count < STANDARD_VECTOR_SIZE) {
		const auto &row = state.rows[state.offset];

		timestamps[count] = row.ts_ns;
		output.data[1].SetValue(count, Value(row.line));

		InsertionOrderPreservingMap<string> label_map;
		for (const auto &label : row.labels) {
			label_map.insert(label.first, label.second);
		}
		output.data[2].SetValue(count, Value::MAP(label_map));

		state.offset++;
		count++;
	}

	output.SetCardinality(count);
}

} // namespace

void RegisterLokiScanFunction(ExtensionLoader &loader) {
	TableFunction loki_scan("loki_scan", {LogicalType::VARCHAR}, LokiScanFunction, LokiScanBind, LokiScanInitGlobal);
	loki_scan.named_parameters["endpoint"] = LogicalType::VARCHAR;
	loki_scan.named_parameters["start"] = LogicalType::TIMESTAMP_TZ;
	loki_scan.named_parameters["end"] = LogicalType::TIMESTAMP_TZ;
	loki_scan.named_parameters["limit"] = LogicalType::BIGINT;
	loader.RegisterFunction(loki_scan);
}

} // namespace duckdb
