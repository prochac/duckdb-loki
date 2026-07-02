#pragma once

#include "loki/loki_types.hpp"

#include <cstdint>
#include <string>

namespace duckdb {
namespace loki {

// Build a query_range request from a LogQL query and time bounds. Pure: no I/O.
// `direction` is Loki's paging direction, "backward" or "forward".
QueryRangeRequest BuildQueryRangeRequest(const std::string &logql, int64_t start_ns, int64_t end_ns, int64_t limit,
                                         const std::string &direction);

// Percent-encode a single query-parameter value per RFC 3986 (unreserved chars kept).
std::string UrlEncode(const std::string &value);

// Serialize a request's params into an `a=b&c=d` query string (values url-encoded).
std::string BuildQueryString(const QueryRangeRequest &request);

} // namespace loki
} // namespace duckdb
