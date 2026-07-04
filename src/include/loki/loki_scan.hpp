#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

// Register both table functions:
//   loki_scan(query, ...)                        — raw LogQL, no predicate translation (§3.3)
//   loki(selector := ..., labels := [...], ...)  — pushdown-driven (§3.2 / §4): WHERE predicates
//       on promoted labels / timestamp / line are translated into the LogQL selector, time
//       bounds, and line filters via pushdown_complex_filter.
// Both resolve a `loki` secret for connection/auth (with inline overrides), page over Loki's
// per-request cap, and accept TIMESTAMP/TIMESTAMPTZ/INTERVAL time bounds.
void RegisterLokiScanFunction(ExtensionLoader &loader);

} // namespace duckdb
