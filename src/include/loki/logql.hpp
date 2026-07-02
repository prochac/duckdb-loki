#pragma once

#include <string>

namespace duckdb {
namespace loki {

// v0.1 (loki_scan) sends the user's LogQL through verbatim, so this is a passthrough.
// It exists as the home for the real selector/pipeline builder that filter pushdown
// (DESIGN.md §4, roadmap v0.4) will grow here — keeping the seam stable for callers.
inline std::string BuildLogQL(const std::string &raw_query) {
	return raw_query;
}

} // namespace loki
} // namespace duckdb
