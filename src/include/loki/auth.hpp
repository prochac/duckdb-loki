#pragma once

#include <string>
#include <utility>
#include <vector>

namespace duckdb {
namespace loki {

// Resolved connection credentials, independent of DuckDB's Secret Manager so the header
// builder below stays pure and unit-testable. Populated by the glue from a `loki` secret
// and/or inline function overrides.
struct AuthConfig {
	std::string token;                                              // bearer / API token
	std::string username;                                           // basic-auth user (e.g. tenant id)
	std::string password;                                           // basic-auth password
	std::string tenant;                                             // -> X-Scope-OrgID
	std::vector<std::pair<std::string, std::string>> extra_headers; // arbitrary passthrough headers
};

// Base64-encode raw bytes (standard alphabet, '=' padding). Exposed for testing.
std::string Base64Encode(const std::string &input);

// Translate an AuthConfig into HTTP request headers. Pure: no I/O.
//   token          -> "Authorization: Bearer <token>"
//   username/pass  -> "Authorization: Basic <base64(user:pass)>"  (only if no token)
//   tenant         -> "X-Scope-OrgID: <tenant>"
//   extra_headers  -> appended verbatim
std::vector<std::pair<std::string, std::string>> BuildAuthHeaders(const AuthConfig &config);

} // namespace loki
} // namespace duckdb
