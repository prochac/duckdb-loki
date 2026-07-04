#pragma once

#include "loki/auth.hpp"

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

#include <cstdint>
#include <string>

namespace duckdb {

// Look up a named table-function parameter, returning nullptr when absent or explicitly NULL.
const Value *FindNamedParam(TableFunctionBindInput &input, const char *name);

// Resolve the Loki connection: endpoint + auth from a `loki` secret (by `secret :=` name, else
// the default secret named `loki`) refined by inline overrides (endpoint / token / username /
// password / tenant / headers). Trailing slashes are trimmed from the endpoint. Throws a
// BinderException naming `fn` when no endpoint can be resolved. Shared by every table function
// (scan + discovery) so connection handling stays identical across the surface.
void ResolveLokiConnection(ClientContext &context, TableFunctionBindInput &input, std::string &endpoint,
                           loki::AuthConfig &auth, const char *fn);

// Resolve a start/end time bound to a nanosecond Unix epoch. An INTERVAL is an offset added to
// `now_ns` (so `-INTERVAL 2 HOUR` means two hours ago); any timestamp-like value is an absolute
// instant. Throws BinderException on a calendar-dependent (month/year) interval.
int64_t ResolveTimeBound(const Value &value, int64_t now_ns);

} // namespace duckdb
