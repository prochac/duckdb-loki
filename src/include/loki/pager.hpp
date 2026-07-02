#pragma once

#include "loki/loki_types.hpp"

#include <cstdint>
#include <set>
#include <string>
#include <vector>

namespace duckdb {
namespace loki {

// Loki caps entries per query_range response (commonly 5000), so returning a complete
// result set larger than that cap requires paging (DESIGN.md §6.4). StreamPager owns the
// time-cursor + de-dup bookkeeping and hands out one request per page; the caller performs
// the HTTP GET and feeds the parsed streams back in. Pure: no DuckDB types, no I/O.
//
// Direction-aware: "backward" walks `end` down toward `start` (newest first); "forward"
// walks `start` up toward `end`. Because query_range's `end` is exclusive / `start` is
// inclusive, the cursor is nudged by 1ns so the boundary nanosecond is re-scanned, and the
// de-dup set drops entries already emitted at that boundary (multiple entries can share an
// identical nanosecond timestamp).
class StreamPager {
public:
	static constexpr int64_t DEFAULT_PER_REQUEST_CAP = 5000;

	StreamPager(std::string logql, int64_t start_ns, int64_t end_ns, const std::string &direction, int64_t user_limit,
	            int64_t per_request_cap = DEFAULT_PER_REQUEST_CAP);

	// Whether all pages have been consumed (limit reached, short page, or no progress).
	bool Done() const {
		return done_;
	}

	// Build the request for the next page. Must not be called once Done() is true.
	QueryRangeRequest NextRequest();

	// Consume a parsed page: de-duplicate against the previous boundary, emit up to the
	// remaining limit, advance the cursor, and update the done flag. Returns the new rows.
	std::vector<LokiRow> Accept(const std::vector<StreamChunk> &page);

private:
	static std::string EntryKey(int64_t ts_ns, const std::string &line,
	                            const std::map<std::string, std::string> &labels);

	std::string logql_;
	int64_t start_ns_;
	int64_t end_ns_;
	bool backward_;
	int64_t user_limit_;
	int64_t per_request_cap_;

	int64_t cursor_ns_;               // moving window edge (end for backward, start for forward)
	int64_t emitted_ = 0;             // rows returned so far, across all pages
	int64_t requested_this_page_ = 0; // per-page limit sent in the last NextRequest()
	bool done_ = false;
	std::set<std::string> boundary_keys_; // entry keys at the previous page's extreme ts
};

} // namespace loki
} // namespace duckdb
