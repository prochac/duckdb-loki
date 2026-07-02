#pragma once

#include "loki/loki_types.hpp"

#include <string>
#include <vector>

namespace duckdb {
namespace loki {

// Parse a Loki query_range response whose `data.resultType` is "streams".
// Returns one StreamChunk per stream. Throws std::runtime_error if the body is not
// valid JSON, lacks `data`, or is not a streams result (the message includes a snippet
// of the offending body so callers can surface a Loki error verbatim).
std::vector<StreamChunk> ParseStreamsResponse(const std::string &json);

} // namespace loki
} // namespace duckdb
