#include "loki/logql.hpp"

#include "test_util.hpp"

using duckdb::loki::BuildSelectorQuery;
using duckdb::loki::EscapeLogQLString;
using duckdb::loki::LabelMatcher;
using duckdb::loki::LineFilter;
using duckdb::loki::LineOp;
using duckdb::loki::MatchOp;
using duckdb::loki::RegexEscapeLiteral;
using duckdb::loki::TryLikeToSubstring;

TEST_CASE("escape logql string") {
	CHECK_EQ(EscapeLogQLString("plain"), std::string("plain"));
	CHECK_EQ(EscapeLogQLString("a\"b"), std::string("a\\\"b"));
	CHECK_EQ(EscapeLogQLString("a\\b"), std::string("a\\\\b"));
	CHECK_EQ(EscapeLogQLString("a\nb"), std::string("a\\nb"));
}

TEST_CASE("regex-escape a literal") {
	CHECK_EQ(RegexEscapeLiteral("plain"), std::string("plain"));
	CHECK_EQ(RegexEscapeLiteral("a.b+c"), std::string("a\\.b\\+c"));
	CHECK_EQ(RegexEscapeLiteral("a|b"), std::string("a\\|b"));
	CHECK_EQ(RegexEscapeLiteral("v1.2(x)"), std::string("v1\\.2\\(x\\)"));
}

TEST_CASE("LIKE to substring: only pure %literal% translates") {
	std::string out;
	CHECK(TryLikeToSubstring("%timeout%", out));
	CHECK_EQ(out, std::string("timeout"));

	// Interior wildcards or non-enclosing patterns do not translate to a substring filter.
	CHECK(!TryLikeToSubstring("timeout%", out));
	CHECK(!TryLikeToSubstring("%timeout", out));
	CHECK(!TryLikeToSubstring("%time_out%", out));
	CHECK(!TryLikeToSubstring("%a%b%", out));
	CHECK(!TryLikeToSubstring("%%", out)); // empty middle -> no useful filter
	CHECK(!TryLikeToSubstring("plain", out));
}

TEST_CASE("build selector: refine a base with matchers") {
	std::vector<LabelMatcher> matchers = {{"level", MatchOp::EQ, "error"}};
	auto q = BuildSelectorQuery("{job=\"api\"}", matchers, {});
	CHECK_EQ(q, std::string("{job=\"api\", level=\"error\"}"));
}

TEST_CASE("build selector: synthesize from matchers when base is empty") {
	std::vector<LabelMatcher> matchers = {{"job", MatchOp::EQ, "api"}, {"pod", MatchOp::RE, "a|b"}};
	auto q = BuildSelectorQuery("", matchers, {});
	CHECK_EQ(q, std::string("{job=\"api\", pod=~\"a|b\"}"));
}

TEST_CASE("build selector: matcher operators and value escaping") {
	std::vector<LabelMatcher> matchers = {{"a", MatchOp::NEQ, "x\"y"}, {"b", MatchOp::NRE, "re"}};
	auto q = BuildSelectorQuery("{job=\"api\"}", matchers, {});
	CHECK_EQ(q, std::string("{job=\"api\", a!=\"x\\\"y\", b!~\"re\"}"));
}

TEST_CASE("build selector: line filters follow the selector in order") {
	std::vector<LineFilter> filters = {{LineOp::CONTAINS, "timeout"}, {LineOp::NOT_MATCH, "^debug"}};
	auto q = BuildSelectorQuery("{job=\"api\"}", {}, filters);
	CHECK_EQ(q, std::string("{job=\"api\"} |= \"timeout\" !~ \"^debug\""));
}

TEST_CASE("build selector: preserves a trailing pipeline written in the base") {
	std::vector<LabelMatcher> matchers = {{"level", MatchOp::EQ, "error"}};
	std::vector<LineFilter> filters = {{LineOp::CONTAINS, "x"}};
	auto q = BuildSelectorQuery("{job=\"api\"} | json", matchers, filters);
	CHECK_EQ(q, std::string("{job=\"api\", level=\"error\"} | json |= \"x\""));
}
