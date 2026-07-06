#pragma once

#include "loki/auth.hpp"
#include "loki/logql.hpp"

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

namespace duckdb {

// Bind data shared by loki_scan (raw LogQL) and loki (pushdown-driven), and reused wholesale by
// the ATTACH catalog's `logs` table (DESIGN.md §3.6), whose GetScanFunction hands DuckDB a
// pre-populated instance directly (the catalog path never calls a table-function bind).
struct LokiScanBindData : public TableFunctionData {
	std::string endpoint;
	std::string query; // raw LogQL (loki_scan); for loki() it's built at init from the selector
	int64_t start_ns = 0;
	int64_t end_ns = 0;
	int64_t limit = 5000;
	std::string direction = "backward";
	loki::AuthConfig auth;
	std::vector<std::string> label_columns; // labels promoted to top-level columns, in declared order

	// --- Filter pushdown (loki(), DESIGN.md §4). Untouched by loki_scan (raw LogQL). ---
	bool pushdown_mode = false; // true for loki(): assemble the query from selector + WHERE
	std::string base_selector;  // mandatory base stream selector, refined by pushdown
	std::vector<loki::LabelMatcher> pushed_matchers;
	std::vector<loki::LineFilter> pushed_line_filters;
	bool has_param_start = false; // start/end came from an explicit start:=/end:= argument
	bool has_param_end = false;
	// Time bounds gathered from WHERE (intersected across predicates); sentinels mean "none".
	int64_t pushed_start_ns = std::numeric_limits<int64_t>::min();
	int64_t pushed_end_ns = std::numeric_limits<int64_t>::max();
};

// Register both table functions:
//   loki_scan(query, ...)                        — raw LogQL, no predicate translation (§3.3)
//   loki(selector := ..., labels := [...], ...)  — pushdown-driven (§3.2 / §4): WHERE predicates
//       on promoted labels / timestamp / line are translated into the LogQL selector, time
//       bounds, and line filters via pushdown_complex_filter.
// Both resolve a `loki` secret for connection/auth (with inline overrides), page over Loki's
// per-request cap, and accept TIMESTAMP/TIMESTAMPTZ/INTERVAL time bounds.
void RegisterLokiScanFunction(ExtensionLoader &loader);

// A `loki()`-style TableFunction (execute + pushdown_complex_filter + projection pushdown, no
// bind) for the ATTACH catalog's `logs` table to reuse the scan core (DESIGN.md §3.6). The caller
// supplies a pre-built LokiScanBindData via TableCatalogEntry::GetScanFunction.
TableFunction MakeLokiCatalogScanFunction();

// Build a pushdown-enabled TableFunction (the shared LokiPushdownComplexFilter that translates
// WHERE label predicates into selector matchers, plus projection pushdown) wired to a
// caller-supplied execute/init. Used by the ATTACH catalog for both the `logs` scan and the
// selector-driven `series` scan, which share the exact same predicate translation (DESIGN.md §3.6).
TableFunction MakeLokiPushdownTableFunction(table_function_t function, table_function_init_global_t init_global);

// Build the fixed `logs` output schema into `columns`, in the SAME order LokiResolveCommon emits
// (timestamp, line, one VARCHAR per promoted label, labels MAP, structured_metadata MAP) so the
// source-column index math in the scan holds for the catalog path.
void BuildLokiLogsColumns(const std::vector<std::string> &label_columns, ColumnList &columns);

} // namespace duckdb
