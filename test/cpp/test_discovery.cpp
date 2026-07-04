#include "test_util.hpp"

#include "loki/http_request.hpp"
#include "loki/parse.hpp"

#include <string>

using duckdb::loki::BuildLabelsRequest;
using duckdb::loki::BuildLabelValuesRequest;
using duckdb::loki::BuildQueryString;
using duckdb::loki::BuildSeriesRequest;
using duckdb::loki::ParseSeriesResponse;
using duckdb::loki::ParseStringArrayResponse;
using duckdb::loki::QueryRangeRequest;

namespace {

// Find a param value in a request; returns true and sets `out` if present.
bool ParamValue(const QueryRangeRequest &req, const std::string &key, std::string &out) {
	for (const auto &p : req.params) {
		if (p.first == key) {
			out = p.second;
			return true;
		}
	}
	return false;
}

} // namespace

TEST_CASE("labels request omits negative time bounds") {
	auto req = BuildLabelsRequest(-1, -1);
	CHECK_EQ(req.path, std::string("/loki/api/v1/labels"));
	CHECK_EQ(req.params.size(), size_t(0));
}

TEST_CASE("labels request includes provided time bounds") {
	auto req = BuildLabelsRequest(100, 200);
	std::string v;
	CHECK(ParamValue(req, "start", v));
	CHECK_EQ(v, std::string("100"));
	CHECK(ParamValue(req, "end", v));
	CHECK_EQ(v, std::string("200"));
}

TEST_CASE("label_values request encodes the name into the path") {
	auto req = BuildLabelValuesRequest("job", -1, -1, "");
	CHECK_EQ(req.path, std::string("/loki/api/v1/label/job/values"));
	std::string v;
	CHECK_FALSE(ParamValue(req, "query", v)); // no query when empty

	// Reserved characters in the label name are percent-encoded in the path segment.
	auto req2 = BuildLabelValuesRequest("a/b", -1, -1, "{job=\"api\"}");
	CHECK_EQ(req2.path, std::string("/loki/api/v1/label/a%2Fb/values"));
	CHECK(ParamValue(req2, "query", v));
	CHECK_EQ(v, std::string("{job=\"api\"}"));
}

TEST_CASE("series request carries the selector as match[]") {
	auto req = BuildSeriesRequest("{job=\"api\"}", 5, -1);
	CHECK_EQ(req.path, std::string("/loki/api/v1/series"));
	std::string v;
	CHECK(ParamValue(req, "match[]", v));
	CHECK_EQ(v, std::string("{job=\"api\"}"));
	CHECK(ParamValue(req, "start", v));
	CHECK_EQ(v, std::string("5"));
	CHECK_FALSE(ParamValue(req, "end", v)); // negative end omitted

	// The `[]` in the key is percent-encoded when serialized (Loki decodes it back to match[]).
	std::string qs = BuildQueryString(req);
	CHECK(qs.find("match%5B%5D=") != std::string::npos);
}

TEST_CASE("string-array response parses label/value lists") {
	auto values = ParseStringArrayResponse(R"({"status":"success","data":["job","level","pod"]})");
	CHECK_EQ(values.size(), size_t(3));
	CHECK_EQ(values[0], std::string("job"));
	CHECK_EQ(values[2], std::string("pod"));
}

TEST_CASE("empty string-array response parses to no rows") {
	auto values = ParseStringArrayResponse(R"({"status":"success","data":[]})");
	CHECK_EQ(values.size(), size_t(0));
}

TEST_CASE("string-array response without array data throws") {
	bool threw = false;
	try {
		ParseStringArrayResponse("no such label matcher");
	} catch (const std::exception &) {
		threw = true;
	}
	CHECK(threw);
}

TEST_CASE("series response parses label sets") {
	const std::string body = R"({
	  "status": "success",
	  "data": [
	    {"job": "api", "level": "error"},
	    {"job": "web"}
	  ]
	})";
	auto series = ParseSeriesResponse(body);
	CHECK_EQ(series.size(), size_t(2));
	CHECK_EQ(series[0].size(), size_t(2));
	CHECK_EQ(series[0].at("job"), std::string("api"));
	CHECK_EQ(series[0].at("level"), std::string("error"));
	CHECK_EQ(series[1].size(), size_t(1));
	CHECK_EQ(series[1].at("job"), std::string("web"));
}

TEST_CASE("series response without array data throws") {
	bool threw = false;
	try {
		ParseSeriesResponse(R"({"status":"error","message":"boom"})");
	} catch (const std::exception &) {
		threw = true;
	}
	CHECK(threw);
}
