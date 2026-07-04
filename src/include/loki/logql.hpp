#pragma once

#include <string>
#include <vector>

namespace duckdb {
namespace loki {

// v0.1 (loki_scan) sends the user's LogQL through verbatim, so this is a passthrough.
// The pushdown-driven `loki()` function (v0.4) instead assembles its query from a base
// stream selector plus the label matchers and line filters translated from the SQL WHERE.
inline std::string BuildLogQL(const std::string &raw_query) {
	return raw_query;
}

// A label matcher inside the stream selector braces, e.g. `job="api"` (EQ) or `level=~"a|b"` (RE).
enum class MatchOp { EQ, NEQ, RE, NRE };
struct LabelMatcher {
	std::string label;
	MatchOp op;
	std::string value; // raw (unescaped) value; RE/NRE hold a regular expression
};

// A line filter following the selector, e.g. `|= "timeout"` (CONTAINS) or `|~ "re"` (MATCH).
enum class LineOp { CONTAINS, NOT_CONTAINS, MATCH, NOT_MATCH };
struct LineFilter {
	LineOp op;
	std::string value; // raw (unescaped) substring, or a regular expression for MATCH/NOT_MATCH
};

// Escape a value for a LogQL double-quoted string (Go-style): backslash, double-quote, and
// the common control characters. Used for both label-matcher and line-filter values.
std::string EscapeLogQLString(const std::string &value);

// Escape RE2 regex metacharacters so a literal string matches itself inside a `=~`/`|~` regex.
std::string RegexEscapeLiteral(const std::string &literal);

// If `pattern` is a pure-substring SQL LIKE pattern (`%literal%` with no interior `%`/`_`
// wildcards and no escapes), set `out` to the literal and return true; otherwise return false.
// Only such patterns translate losslessly to a Loki `|=`/`!=` substring line filter.
bool TryLikeToSubstring(const std::string &pattern, std::string &out);

// Assemble a LogQL query: the base stream selector refined with extra label matchers, followed
// by the line filters. `base_selector` is a stream selector like `{job="api"}` (or empty, in
// which case one is synthesized purely from `matchers`). Pure: no I/O, unit-tested.
std::string BuildSelectorQuery(const std::string &base_selector, const std::vector<LabelMatcher> &matchers,
                               const std::vector<LineFilter> &line_filters);

} // namespace loki
} // namespace duckdb
