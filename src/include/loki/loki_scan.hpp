#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

// Register the `loki_scan(query, endpoint := ..., start := ..., end := ..., limit := ...)`
// table function on the given loader.
void RegisterLokiScanFunction(ExtensionLoader &loader);

} // namespace duckdb
