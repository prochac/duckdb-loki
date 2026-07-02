#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

// Register the raw-LogQL table function:
//   loki_scan(query, endpoint := ..., secret := ..., token/username/password/tenant := ...,
//             headers := MAP{...}, start := ..., end := ..., limit := ..., direction := ...)
// Resolves a `loki` secret for connection/auth, pages over Loki's per-request cap, and
// accepts TIMESTAMP/TIMESTAMPTZ/INTERVAL time bounds.
void RegisterLokiScanFunction(ExtensionLoader &loader);

} // namespace duckdb
