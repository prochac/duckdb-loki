#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

namespace duckdb {
namespace loki {

// One Loki stream: its label set plus the (nanosecond-epoch, line) entries it carries.
// This mirrors a single element of the `data.result[]` array in a "streams" response.
struct StreamChunk {
	std::map<std::string, std::string> labels;
	std::vector<std::pair<int64_t, std::string>> values; // (timestamp_ns, line)
};

// One flattened output row: a single stream entry paired with its stream's labels.
// Produced by the pager from StreamChunks and drained into the output vector by the glue.
struct LokiRow {
	int64_t ts_ns;
	std::string line;
	std::map<std::string, std::string> labels;
};

// A fully-built request to Loki's query_range endpoint: a path and ordered, unencoded
// query parameters. Kept free of any HTTP-client type so it stays unit-testable.
struct QueryRangeRequest {
	std::string path;
	std::vector<std::pair<std::string, std::string>> params;
};

} // namespace loki
} // namespace duckdb
