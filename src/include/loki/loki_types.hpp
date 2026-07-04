#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

namespace duckdb {
namespace loki {

// One entry within a stream: its nanosecond-epoch timestamp, line, and optional per-entry
// structured metadata (the newer Loki feature — the optional 3rd element of a `values` pair).
struct StreamEntry {
	int64_t ts_ns;
	std::string line;
	std::map<std::string, std::string> structured_metadata; // usually empty
};

// One Loki stream: its label set plus the entries it carries.
// This mirrors a single element of the `data.result[]` array in a "streams" response.
struct StreamChunk {
	std::map<std::string, std::string> labels;
	std::vector<StreamEntry> values;
};

// One flattened output row: a single stream entry paired with its stream's labels.
// Produced by the pager from StreamChunks and drained into the output vector by the glue.
struct LokiRow {
	int64_t ts_ns;
	std::string line;
	std::map<std::string, std::string> labels;
	std::map<std::string, std::string> structured_metadata;
};

// A fully-built request to Loki's query_range endpoint: a path and ordered, unencoded
// query parameters. Kept free of any HTTP-client type so it stays unit-testable.
struct QueryRangeRequest {
	std::string path;
	std::vector<std::pair<std::string, std::string>> params;
};

} // namespace loki
} // namespace duckdb
