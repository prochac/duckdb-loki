#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace duckdb {
namespace loki {

struct HttpResponse {
	int64_t status = 0; // HTTP status code; 0 if the request never completed
	std::string body;   // response body (on success or error status)
	std::string error;  // transport-level error message; empty if the request completed
};

// Issue an HTTP(S) GET. `endpoint` is scheme://host[:port] (TLS is used for https://).
// `path` is the URL path, `query` an already-encoded query string (no leading '?').
// This is the one impure seam over cpp-httplib; everything above it is unit-testable.
HttpResponse HttpGet(const std::string &endpoint, const std::string &path, const std::string &query,
                     const std::vector<std::pair<std::string, std::string>> &headers);

} // namespace loki
} // namespace duckdb
