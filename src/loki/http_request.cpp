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
