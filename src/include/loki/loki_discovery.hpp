#pragma once

#include "loki/auth.hpp"
#include "loki/loki_types.hpp"

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

// Which discovery endpoint a scan targets — decides the request built and the output row shape.
enum class DiscoveryKind { LABELS, LABEL_VALUES, SERIES };

// Bind data for the discovery scans, reused by the ATTACH catalog's read-only `labels` table
// (DESIGN.md §3.6): its GetScanFunction hands DuckDB a pre-built instance directly.
struct LokiDiscoveryBindData : public TableFunctionData {
	DiscoveryKind kind;
	std::string endpoint;
	loki::AuthConfig auth;
	loki::QueryRangeRequest request; // fully built at bind; no network until the scan runs
};

// Register the label-discovery table functions (DESIGN.md §3.4, roadmap v0.5):
//   loki_labels(...)                 — GET /labels: one row per label name
//   loki_label_values(name, ...)     — GET /label/{name}/values: one row per value
//   loki_series(match, ...)          — GET /series: one MAP row per matching series
// All resolve a `loki` secret for connection/auth (with inline overrides) and accept an optional
// TIMESTAMP/TIMESTAMPTZ/INTERVAL start/end window.
void RegisterLokiDiscoveryFunctions(ExtensionLoader &loader);

// A discovery-backed TableFunction (execute + init, no bind) for the ATTACH catalog to reuse
// (DESIGN.md §3.6): the caller supplies a pre-built LokiDiscoveryBindData via GetScanFunction.
TableFunction MakeLokiDiscoveryScanFunction();

} // namespace duckdb
