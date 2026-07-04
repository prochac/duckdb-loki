#include "loki/http_request.hpp"

#include <cctype>
#include <cstdio>
#include <string>

namespace duckdb {
namespace loki {

std::string UrlEncode(const std::string &value) {
	std::string out;
	out.reserve(value.size() * 3);
	for (unsigned char c : value) {
		if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
			out.push_back(static_cast<char>(c));
		} else {
			char buf[4];
			std::snprintf(buf, sizeof(buf), "%%%02X", c);
			out.append(buf);
		}
	}
	return out;
}

QueryRangeRequest BuildQueryRangeRequest(const std::string &logql, int64_t start_ns, int64_t end_ns, int64_t limit,
                                         const std::string &direction) {
	QueryRangeRequest request;
	request.path = "/loki/api/v1/query_range";
	request.params.emplace_back("query", logql);
	request.params.emplace_back("start", std::to_string(start_ns));
	request.params.emplace_back("end", std::to_string(end_ns));
	request.params.emplace_back("limit", std::to_string(limit));
	request.params.emplace_back("direction", direction);
	return request;
}

namespace {

// Append `start`/`end` nanosecond bounds to a discovery request, skipping negative sentinels
// (which mean "let Loki use its default window").
void AppendTimeBounds(QueryRangeRequest &request, int64_t start_ns, int64_t end_ns) {
	if (start_ns >= 0) {
		request.params.emplace_back("start", std::to_string(start_ns));
	}
	if (end_ns >= 0) {
		request.params.emplace_back("end", std::to_string(end_ns));
	}
}

} // namespace

QueryRangeRequest BuildLabelsRequest(int64_t start_ns, int64_t end_ns) {
	QueryRangeRequest request;
	request.path = "/loki/api/v1/labels";
	AppendTimeBounds(request, start_ns, end_ns);
	return request;
}

QueryRangeRequest BuildLabelValuesRequest(const std::string &name, int64_t start_ns, int64_t end_ns,
                                          const std::string &query) {
	QueryRangeRequest request;
	// The label name is a path segment; url-encode it so names with reserved characters are safe.
	request.path = "/loki/api/v1/label/" + UrlEncode(name) + "/values";
	AppendTimeBounds(request, start_ns, end_ns);
	if (!query.empty()) {
		request.params.emplace_back("query", query);
	}
	return request;
}

QueryRangeRequest BuildSeriesRequest(const std::string &match, int64_t start_ns, int64_t end_ns) {
	QueryRangeRequest request;
	request.path = "/loki/api/v1/series";
	// Loki takes the selector via the repeatable `match[]` parameter; we send exactly one.
	request.params.emplace_back("match[]", match);
	AppendTimeBounds(request, start_ns, end_ns);
	return request;
}

std::string BuildQueryString(const QueryRangeRequest &request) {
	std::string out;
	for (const auto &param : request.params) {
		if (!out.empty()) {
			out.push_back('&');
		}
		out.append(UrlEncode(param.first));
		out.push_back('=');
		out.append(UrlEncode(param.second));
	}
	return out;
}

} // namespace loki
} // namespace duckdb
