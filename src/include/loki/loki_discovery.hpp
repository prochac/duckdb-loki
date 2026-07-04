#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

// Register the label-discovery table functions (DESIGN.md §3.4, roadmap v0.5):
//   loki_labels(...)                 — GET /labels: one row per label name
//   loki_label_values(name, ...)     — GET /label/{name}/values: one row per value
//   loki_series(match, ...)          — GET /series: one MAP row per matching series
// All resolve a `loki` secret for connection/auth (with inline overrides) and accept an optional
// TIMESTAMP/TIMESTAMPTZ/INTERVAL start/end window.
void RegisterLokiDiscoveryFunctions(ExtensionLoader &loader);

} // namespace duckdb
