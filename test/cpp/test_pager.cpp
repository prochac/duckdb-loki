#include "test_util.hpp"

#include "loki/pager.hpp"

#include <string>
#include <vector>

using duckdb::loki::LokiRow;
using duckdb::loki::StreamChunk;
using duckdb::loki::StreamPager;

namespace {

// One single-stream page from (ts_ns, line) pairs, newest-first as Loki returns for backward.
StreamChunk MakeChunk(const std::vector<std::pair<int64_t, std::string>> &entries) {
	StreamChunk chunk;
	chunk.labels = {{"job", "x"}};
	chunk.values = entries;
	return chunk;
}

std::vector<std::string> Lines(const std::vector<LokiRow> &rows) {
	std::vector<std::string> out;
	for (const auto &row : rows) {
		out.push_back(row.line);
	}
	return out;
}

} // namespace

TEST_CASE("single page under cap is done immediately") {
	StreamPager pager("{job=\"x\"}", 0, 1000, "backward", 100, 5000);
	CHECK(!pager.Done());
	auto req = pager.NextRequest();
	CHECK_EQ(req.path, std::string("/loki/api/v1/query_range"));
	auto rows = pager.Accept({MakeChunk({{100, "b"}, {90, "a"}})});
	CHECK(pager.Done()); // 2 < requested 100
	CHECK_EQ(rows.size(), size_t(2));
	CHECK_EQ(rows[0].line, std::string("b"));
	CHECK_EQ(rows[1].line, std::string("a"));
}

TEST_CASE("backward paging de-dups the boundary nanosecond") {
	// per_request_cap 2 forces paging; entries at the boundary ns must not double-count.
	StreamPager pager("{job=\"x\"}", 0, 1000, "backward", 100, 2);

	pager.NextRequest();
	auto p1 = pager.Accept({MakeChunk({{100, "c"}, {90, "b"}})});
	CHECK(!pager.Done());
	CHECK(Lines(p1) == (std::vector<std::string> {"c", "b"}));

	pager.NextRequest();
	// Loki re-serves ts=90 "b" (boundary) plus a new older entry; the dup is dropped.
	auto p2 = pager.Accept({MakeChunk({{90, "b"}, {80, "a"}})});
	CHECK(!pager.Done());
	CHECK(Lines(p2) == (std::vector<std::string> {"a"}));

	pager.NextRequest();
	// Final short page (1 < cap 2) — only the already-seen boundary entry.
	auto p3 = pager.Accept({MakeChunk({{80, "a"}})});
	CHECK(pager.Done());
	CHECK_EQ(p3.size(), size_t(0));
}

TEST_CASE("distinct lines at same ns are both kept") {
	StreamPager pager("{job=\"x\"}", 0, 1000, "backward", 100, 5000);
	pager.NextRequest();
	auto rows = pager.Accept({MakeChunk({{100, "a"}, {100, "b"}})});
	CHECK(pager.Done());
	CHECK_EQ(rows.size(), size_t(2));
}

TEST_CASE("user limit caps total rows") {
	StreamPager pager("{job=\"x\"}", 0, 1000, "backward", 1, 5000);
	pager.NextRequest();
	auto rows = pager.Accept({MakeChunk({{100, "c"}, {90, "b"}, {80, "a"}})});
	CHECK(pager.Done());
	CHECK_EQ(rows.size(), size_t(1));
	CHECK_EQ(rows[0].line, std::string("c"));
}

TEST_CASE("limit is hit exactly across a page boundary") {
	// The boundary entry is re-served and de-duped; the pager must compensate so the total
	// lands on the user's limit rather than falling one short per boundary.
	StreamPager pager("{job=\"x\"}", 0, 1000, "backward", 3, 2);
	pager.NextRequest();
	auto p1 = pager.Accept({MakeChunk({{100, "c"}, {90, "b"}})});
	CHECK(!pager.Done());
	CHECK_EQ(p1.size(), size_t(2));

	pager.NextRequest();
	auto p2 = pager.Accept({MakeChunk({{90, "b"}, {80, "a"}})});
	CHECK(pager.Done());
	CHECK_EQ(p2.size(), size_t(1)); // one new row -> 3 total, exactly the limit
	CHECK_EQ(p2[0].line, std::string("a"));
}

TEST_CASE("forward paging advances start upward") {
	StreamPager pager("{job=\"x\"}", 0, 1000, "forward", 100, 2);
	pager.NextRequest();
	auto p1 = pager.Accept({MakeChunk({{80, "a"}, {90, "b"}})});
	CHECK(!pager.Done());
	CHECK(Lines(p1) == (std::vector<std::string> {"a", "b"}));

	pager.NextRequest();
	auto p2 = pager.Accept({MakeChunk({{90, "b"}, {100, "c"}})}); // 90 re-served at boundary
	CHECK(!pager.Done());
	CHECK(Lines(p2) == (std::vector<std::string> {"c"}));
}

TEST_CASE("full page all sharing one ns terminates") {
	// Guards against an infinite loop when the cursor cannot advance past a single ns.
	StreamPager pager("{job=\"x\"}", 0, 1000, "backward", 100, 2);
	pager.NextRequest();
	auto p1 = pager.Accept({MakeChunk({{100, "a"}, {100, "b"}})});
	CHECK(!pager.Done()); // cursor moved 1000 -> 101
	CHECK_EQ(p1.size(), size_t(2));

	pager.NextRequest();
	// Loki keeps re-serving the same boundary ns; both entries de-dup and the cursor
	// cannot advance (101 -> 101), so the pager must stop rather than loop forever.
	auto p2 = pager.Accept({MakeChunk({{100, "a"}, {100, "b"}})});
	CHECK(pager.Done());
	CHECK_EQ(p2.size(), size_t(0));
}
