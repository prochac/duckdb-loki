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

// A fully-built request to Loki's query_range endpoint: a path and ordered, unencoded
// query parameters. Kept free of any HTTP-client type so it stays unit-testable.
struct QueryRangeRequest {
	std::string path;
	std::vector<std::pair<std::string, std::string>> params;
};

} // namespace loki
} // namespace duckdb
