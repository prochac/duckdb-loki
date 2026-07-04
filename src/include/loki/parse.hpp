#pragma once

#include "loki/loki_types.hpp"

#include <map>
#include <string>
#include <vector>

namespace duckdb {
namespace loki {

// Parse a Loki query_range response whose `data.resultType` is "streams".
// Returns one StreamChunk per stream. Throws std::runtime_error if the body is not
// valid JSON, lacks `data`, or is not a streams result (the message includes a snippet
// of the offending body so callers can surface a Loki error verbatim).
std::vector<StreamChunk> ParseStreamsResponse(const std::string &json);

// Parse a Loki discovery response whose `data` is a flat array of strings — used by the
// /labels and /label/{name}/values endpoints. Throws std::runtime_error on invalid JSON or
// a missing/`non-array `data` field (the message embeds a snippet of the body).
std::vector<std::string> ParseStringArrayResponse(const std::string &json);

// Parse a Loki /series response whose `data` is an array of label-set objects. Returns one
// map per matching series. Throws std::runtime_error on invalid JSON or a missing/non-array
// `data` field.
std::vector<std::map<std::string, std::string>> ParseSeriesResponse(const std::string &json);

} // namespace loki
} // namespace duckdb
