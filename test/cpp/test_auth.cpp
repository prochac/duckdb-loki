#include "test_util.hpp"

#include "loki/auth.hpp"

using duckdb::loki::AuthConfig;
using duckdb::loki::Base64Encode;
using duckdb::loki::BuildAuthHeaders;

TEST_CASE("base64 standard vectors") {
	CHECK_EQ(Base64Encode(""), std::string(""));
	CHECK_EQ(Base64Encode("f"), std::string("Zg=="));
	CHECK_EQ(Base64Encode("fo"), std::string("Zm8="));
	CHECK_EQ(Base64Encode("foo"), std::string("Zm9v"));
	CHECK_EQ(Base64Encode("foob"), std::string("Zm9vYg=="));
	CHECK_EQ(Base64Encode("fooba"), std::string("Zm9vYmE="));
	CHECK_EQ(Base64Encode("foobar"), std::string("Zm9vYmFy"));
	CHECK_EQ(Base64Encode("user:pass"), std::string("dXNlcjpwYXNz"));
}

TEST_CASE("bearer token header") {
	AuthConfig cfg;
	cfg.token = "glc_abc";
	auto headers = BuildAuthHeaders(cfg);
	CHECK_EQ(headers.size(), size_t(1));
	CHECK_EQ(headers[0].first, std::string("Authorization"));
	CHECK_EQ(headers[0].second, std::string("Bearer glc_abc"));
}

TEST_CASE("basic auth header when no token") {
	AuthConfig cfg;
	cfg.username = "user";
	cfg.password = "pass";
	auto headers = BuildAuthHeaders(cfg);
	CHECK_EQ(headers.size(), size_t(1));
	CHECK_EQ(headers[0].first, std::string("Authorization"));
	CHECK_EQ(headers[0].second, std::string("Basic dXNlcjpwYXNz"));
}

TEST_CASE("token wins over basic") {
	AuthConfig cfg;
	cfg.token = "tok";
	cfg.username = "user";
	cfg.password = "pass";
	auto headers = BuildAuthHeaders(cfg);
	CHECK_EQ(headers.size(), size_t(1));
	CHECK_EQ(headers[0].second, std::string("Bearer tok"));
}

TEST_CASE("tenant and extra headers") {
	AuthConfig cfg;
	cfg.token = "tok";
	cfg.tenant = "1234";
	cfg.extra_headers = {{"X-Api-Key", "k"}};
	auto headers = BuildAuthHeaders(cfg);
	CHECK_EQ(headers.size(), size_t(3));
	CHECK_EQ(headers[0].second, std::string("Bearer tok"));
	CHECK_EQ(headers[1].first, std::string("X-Scope-OrgID"));
	CHECK_EQ(headers[1].second, std::string("1234"));
	CHECK_EQ(headers[2].first, std::string("X-Api-Key"));
}

TEST_CASE("empty config yields no headers") {
	AuthConfig cfg;
	CHECK_EQ(BuildAuthHeaders(cfg).size(), size_t(0));
}
