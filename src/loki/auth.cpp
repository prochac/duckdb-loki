#include "loki/auth.hpp"

#include <cstdint>

namespace duckdb {
namespace loki {

std::string Base64Encode(const std::string &input) {
	static const char kAlphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
	std::string out;
	out.reserve(((input.size() + 2) / 3) * 4);

	size_t i = 0;
	const size_t n = input.size();
	while (i + 3 <= n) {
		uint32_t triple = (static_cast<uint8_t>(input[i]) << 16) | (static_cast<uint8_t>(input[i + 1]) << 8) |
		                  static_cast<uint8_t>(input[i + 2]);
		out.push_back(kAlphabet[(triple >> 18) & 0x3F]);
		out.push_back(kAlphabet[(triple >> 12) & 0x3F]);
		out.push_back(kAlphabet[(triple >> 6) & 0x3F]);
		out.push_back(kAlphabet[triple & 0x3F]);
		i += 3;
	}

	const size_t rem = n - i;
	if (rem == 1) {
		uint32_t triple = static_cast<uint8_t>(input[i]) << 16;
		out.push_back(kAlphabet[(triple >> 18) & 0x3F]);
		out.push_back(kAlphabet[(triple >> 12) & 0x3F]);
		out.push_back('=');
		out.push_back('=');
	} else if (rem == 2) {
		uint32_t triple = (static_cast<uint8_t>(input[i]) << 16) | (static_cast<uint8_t>(input[i + 1]) << 8);
		out.push_back(kAlphabet[(triple >> 18) & 0x3F]);
		out.push_back(kAlphabet[(triple >> 12) & 0x3F]);
		out.push_back(kAlphabet[(triple >> 6) & 0x3F]);
		out.push_back('=');
	}

	return out;
}

std::vector<std::pair<std::string, std::string>> BuildAuthHeaders(const AuthConfig &config) {
	std::vector<std::pair<std::string, std::string>> headers;

	if (!config.token.empty()) {
		headers.emplace_back("Authorization", "Bearer " + config.token);
	} else if (!config.username.empty() || !config.password.empty()) {
		headers.emplace_back("Authorization", "Basic " + Base64Encode(config.username + ":" + config.password));
	}

	if (!config.tenant.empty()) {
		headers.emplace_back("X-Scope-OrgID", config.tenant);
	}

	for (const auto &header : config.extra_headers) {
		headers.push_back(header);
	}

	return headers;
}

} // namespace loki
} // namespace duckdb
