#include "loki/pager.hpp"

#include "loki/http_request.hpp"

#include <algorithm>
#include <utility>

namespace duckdb {
namespace loki {

StreamPager::StreamPager(std::string logql, int64_t start_ns, int64_t end_ns, const std::string &direction,
                         int64_t user_limit, int64_t per_request_cap)
    : logql_(std::move(logql)), start_ns_(start_ns), end_ns_(end_ns), backward_(direction != "forward"),
      user_limit_(user_limit), per_request_cap_(per_request_cap) {
	cursor_ns_ = backward_ ? end_ns_ : start_ns_;
}

std::string StreamPager::EntryKey(int64_t ts_ns, const std::string &line,
                                  const std::map<std::string, std::string> &labels) {
	std::string key = std::to_string(ts_ns);
	key.push_back('\x1f');
	key.append(line);
	// labels is a std::map, so iteration order is deterministic for a stable key.
	for (const auto &label : labels) {
		key.push_back('\x1e');
		key.append(label.first);
		key.push_back('\x1d');
		key.append(label.second);
	}
	return key;
}

QueryRangeRequest StreamPager::NextRequest() {
	int64_t remaining = user_limit_ - emitted_;
	// The boundary nanosecond is re-served every page and then de-duplicated, so ask for that
	// many extra entries to still net `remaining` NEW rows and hit the user's limit exactly.
	int64_t want = remaining + static_cast<int64_t>(boundary_keys_.size());
	requested_this_page_ = std::min(want, per_request_cap_);

	int64_t start = backward_ ? start_ns_ : cursor_ns_;
	int64_t end = backward_ ? cursor_ns_ : end_ns_;
	return BuildQueryRangeRequest(logql_, start, end, requested_this_page_, backward_ ? "backward" : "forward");
}

std::vector<LokiRow> StreamPager::Accept(const std::vector<StreamChunk> &page) {
	std::vector<LokiRow> rows;

	int64_t page_count = 0;
	bool have_extreme = false;
	int64_t extreme_ns = 0;

	for (const auto &chunk : page) {
		for (const auto &entry : chunk.values) {
			const int64_t ts = entry.first;
			page_count++;

			if (!have_extreme) {
				extreme_ns = ts;
				have_extreme = true;
			} else if (backward_) {
				extreme_ns = std::min(extreme_ns, ts);
			} else {
				extreme_ns = std::max(extreme_ns, ts);
			}

			if (emitted_ >= user_limit_) {
				continue; // still count the raw entry (for short-page detection), but don't emit
			}
			std::string key = EntryKey(ts, entry.second, chunk.labels);
			if (boundary_keys_.count(key)) {
				continue; // already emitted at the previous boundary
			}
			LokiRow row;
			row.ts_ns = ts;
			row.line = entry.second;
			row.labels = chunk.labels;
			rows.push_back(std::move(row));
			emitted_++;
		}
	}

	// Exhausted: Loki returned fewer than we asked for, or the user's limit is met.
	if (page_count < requested_this_page_ || emitted_ >= user_limit_) {
		done_ = true;
		return rows;
	}

	// Advance the cursor to the boundary ns (nudged by 1 so it is re-scanned next page),
	// and remember which entries at that ns we have already emitted so they de-dup.
	const int64_t next_cursor = backward_ ? extreme_ns + 1 : extreme_ns;
	const bool progressed = backward_ ? (next_cursor < cursor_ns_) : (next_cursor > cursor_ns_);
	if (!progressed) {
		done_ = true; // cursor stuck (e.g. a full page all sharing one nanosecond)
		return rows;
	}
	cursor_ns_ = next_cursor;

	boundary_keys_.clear();
	for (const auto &chunk : page) {
		for (const auto &entry : chunk.values) {
			if (entry.first == extreme_ns) {
				boundary_keys_.insert(EntryKey(entry.first, entry.second, chunk.labels));
			}
		}
	}

	return rows;
}

} // namespace loki
} // namespace duckdb
