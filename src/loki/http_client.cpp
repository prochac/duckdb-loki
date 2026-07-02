// NOTE: this translation unit is compiled with CPPHTTPLIB_OPENSSL_SUPPORT (see CMakeLists.txt)
// so that https:// endpoints (e.g. Grafana Cloud) go through cpp-httplib's TLS client. OpenSSL
// is already linked into both extension targets.
#include "loki/http_client.hpp"

#include <httplib.h>

namespace duckdb {
namespace loki {

HttpResponse HttpGet(const std::string &endpoint, const std::string &path, const std::string &query,
                     const std::vector<std::pair<std::string, std::string>> &headers) {
	HttpResponse out;

	httplib::Client client(endpoint);
	client.set_connection_timeout(10, 0); // 10s
	client.set_read_timeout(60, 0);       // 60s
	client.set_follow_location(true);

	httplib::Headers header_map;
	for (const auto &header : headers) {
		header_map.emplace(header.first, header.second);
	}

	std::string target = query.empty() ? path : path + "?" + query;
	auto res = client.Get(target, header_map);
	if (!res) {
		out.error = httplib::to_string(res.error());
		return out;
	}
	out.status = res->status;
	out.body = std::move(res->body);
	return out;
}

} // namespace loki
} // namespace duckdb
