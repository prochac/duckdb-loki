#include "loki/logql.hpp"

#include <cstddef>

namespace duckdb {
namespace loki {

namespace {

std::string Trim(const std::string &s) {
	size_t begin = 0;
	size_t end = s.size();
	while (begin < end && (s[begin] == ' ' || s[begin] == '\t' || s[begin] == '\n' || s[begin] == '\r')) {
		begin++;
	}
	while (end > begin && (s[end - 1] == ' ' || s[end - 1] == '\t' || s[end - 1] == '\n' || s[end - 1] == '\r')) {
		end--;
	}
	return s.substr(begin, end - begin);
}

std::string MatcherToString(const LabelMatcher &m) {
	const char *op = "=";
	switch (m.op) {
	case MatchOp::EQ:
		op = "=";
		break;
	case MatchOp::NEQ:
		op = "!=";
		break;
	case MatchOp::RE:
		op = "=~";
		break;
	case MatchOp::NRE:
		op = "!~";
		break;
	}
	return m.label + op + "\"" + EscapeLogQLString(m.value) + "\"";
}

std::string LineFilterToString(const LineFilter &f) {
	const char *op = "|=";
	switch (f.op) {
	case LineOp::CONTAINS:
		op = "|=";
		break;
	case LineOp::NOT_CONTAINS:
		op = "!=";
		break;
	case LineOp::MATCH:
		op = "|~";
		break;
	case LineOp::NOT_MATCH:
		op = "!~";
		break;
	}
	return std::string(op) + " \"" + EscapeLogQLString(f.value) + "\"";
}

} // namespace

std::string EscapeLogQLString(const std::string &value) {
	std::string out;
	out.reserve(value.size() + 2);
	for (char c : value) {
		switch (c) {
		case '\\':
			out += "\\\\";
			break;
		case '"':
			out += "\\\"";
			break;
		case '\n':
			out += "\\n";
			break;
		case '\t':
			out += "\\t";
			break;
		case '\r':
			out += "\\r";
			break;
		default:
			out += c;
			break;
		}
	}
	return out;
}

std::string RegexEscapeLiteral(const std::string &literal) {
	std::string out;
	out.reserve(literal.size() * 2);
	for (char c : literal) {
		switch (c) {
		case '\\':
		case '.':
		case '+':
		case '*':
		case '?':
		case '(':
		case ')':
		case '|':
		case '[':
		case ']':
		case '{':
		case '}':
		case '^':
		case '$':
			out += '\\';
			out += c;
			break;
		default:
			out += c;
			break;
		}
	}
	return out;
}

bool TryLikeToSubstring(const std::string &pattern, std::string &out) {
	// Must be bracketed by leading and trailing `%` with a non-empty literal middle.
	if (pattern.size() < 3 || pattern.front() != '%' || pattern.back() != '%') {
		return false;
	}
	std::string middle = pattern.substr(1, pattern.size() - 2);
	// Any interior LIKE wildcard means this is not a pure substring match (the 2-argument
	// `~~`/`!~~` operators have no escape character, so `%`/`_` here are always wildcards).
	for (char c : middle) {
		if (c == '%' || c == '_') {
			return false;
		}
	}
	out = std::move(middle);
	return true;
}

std::string BuildSelectorQuery(const std::string &base_selector, const std::vector<LabelMatcher> &matchers,
                               const std::vector<LineFilter> &line_filters) {
	std::string trimmed = Trim(base_selector);

	// Split the base into the stream selector's inner matchers and any trailing pipeline the
	// user wrote directly in the selector (e.g. `{job="api"} | json`), which we preserve as-is.
	std::string inner;
	std::string rest;
	size_t open = trimmed.find('{');
	if (open == std::string::npos) {
		inner = trimmed; // empty, or a bare selector body without braces
	} else {
		size_t close = trimmed.rfind('}');
		if (close == std::string::npos || close < open) {
			inner = trimmed.substr(open + 1);
		} else {
			inner = trimmed.substr(open + 1, close - open - 1);
			rest = trimmed.substr(close + 1);
		}
	}
	inner = Trim(inner);

	std::string selector = "{";
	bool first = true;
	if (!inner.empty()) {
		selector += inner;
		first = false;
	}
	for (const auto &m : matchers) {
		if (!first) {
			selector += ", ";
		}
		selector += MatcherToString(m);
		first = false;
	}
	selector += "}";

	std::string query = selector;
	std::string trimmed_rest = Trim(rest);
	if (!trimmed_rest.empty()) {
		query += " " + trimmed_rest;
	}
	for (const auto &f : line_filters) {
		query += " " + LineFilterToString(f);
	}
	return query;
}

} // namespace loki
} // namespace duckdb
