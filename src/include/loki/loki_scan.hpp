#pragma once

#include "loki/auth.hpp"
#include "loki/logql.hpp"

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <cstdint>
#include <limits>
#include <map>
#include <string>
#include <vector>

namespace duckdb {

// The default query window is the last hour (DESIGN.md §6.6). Shared so the table-function bind and
// the ATTACH catalog's GetScanFunction, which bypasses that bind, cannot drift apart.
constexpr int64_t LOKI_NANOS_PER_HOUR = 3600LL * 1000LL * 1000LL * 1000LL;

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
	// What to call this scan in error messages. The execute path is shared by loki_scan(), loki()
	// and the catalog's `logs`/`series` tables, so it must not hardcode one of them.
	std::string fn_name = "loki_scan";
	// How to fix a violation of the mandatory-selector rule (DESIGN.md §4.3). The function path can
	// suggest `selector :=`; the catalog path has no such argument and must suggest a WHERE instead.
	std::string selector_hint = "pass selector := '{job=\"api\"}' or add a WHERE equality/IN on a "
	                            "promoted label column (labels := [...])";
	// Series-only: the catalog's `series` table enumerates GET /series in one shot — that endpoint
	// takes no limit and no cursor, so there is nothing to page. This caps how many rows we will
	// materialize before erroring out (DESIGN.md §3.6). Ignored by the logs scan, which pages.
	idx_t series_max_rows = 100000;

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

// Build a MAP(VARCHAR, VARCHAR) Value from a string map (labels or structured metadata). Shared by
// the logs scan, the discovery scans, and the catalog's `series` scan.
Value LokiMapFromStrings(const std::map<std::string, std::string> &m);

// True for the fixed column names the `logs`/`series` schemas always emit: timestamp, line, labels,
// structured_metadata. A promoted label sharing one of them collides — DuckDB's ColumnList rejects
// duplicates with an opaque "Column with name X already exists!", which for auto-discovered labels
// would mean a perfectly ordinary Loki deployment could make ATTACH fail. The comparison is
// case-insensitive because ColumnList's name map is.
bool LokiIsReservedColumnName(const std::string &name);

} // namespace duckdb
