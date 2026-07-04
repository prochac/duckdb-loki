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

// Discovery requests (DESIGN.md §3.4 / §5). A negative bound means "omit it" and let Loki
// apply its own default window; a non-empty optional string is likewise only sent when set.

// GET /loki/api/v1/labels — label names.
QueryRangeRequest BuildLabelsRequest(int64_t start_ns, int64_t end_ns);

// GET /loki/api/v1/label/{name}/values — values for one label. `name` is placed in the path
// (url-encoded); an optional `query` selector narrows which series contribute values.
QueryRangeRequest BuildLabelValuesRequest(const std::string &name, int64_t start_ns, int64_t end_ns,
                                          const std::string &query);

// GET /loki/api/v1/series — series (label sets) matching a selector, passed as `match[]`.
QueryRangeRequest BuildSeriesRequest(const std::string &match, int64_t start_ns, int64_t end_ns);

// Percent-encode a single query-parameter value per RFC 3986 (unreserved chars kept).
std::string UrlEncode(const std::string &value);

// Serialize a request's params into an `a=b&c=d` query string (values url-encoded).
std::string BuildQueryString(const QueryRangeRequest &request);

} // namespace loki
} // namespace duckdb
