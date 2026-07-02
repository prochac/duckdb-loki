#include "test_util.hpp"

#include "loki/parse.hpp"

#include <stdexcept>
#include <string>

using duckdb::loki::ParseStreamsResponse;
using duckdb::loki::StreamChunk;

TEST_CASE("parse streams response") {
	const std::string body = R"({
	  "status": "success",
	  "data": {
	    "resultType": "streams",
	    "result": [
	      {
	        "stream": {"job": "api", "level": "error"},
	        "values": [
	          ["1700000000000000001", "boom"],
	          ["1700000000000000000", "warn"]
	        ]
	      }
	    ]
	  }
	})";

	auto chunks = ParseStreamsResponse(body);
	CHECK_EQ(chunks.size(), size_t(1));
	CHECK_EQ(chunks[0].labels.size(), size_t(2));
	CHECK_EQ(chunks[0].labels.at("job"), std::string("api"));
	CHECK_EQ(chunks[0].labels.at("level"), std::string("error"));
	CHECK_EQ(chunks[0].values.size(), size_t(2));
	CHECK_EQ(chunks[0].values[0].first, int64_t(1700000000000000001LL));
	CHECK_EQ(chunks[0].values[0].second, std::string("boom"));
	CHECK_EQ(chunks[0].values[1].first, int64_t(1700000000000000000LL));
}

TEST_CASE("empty streams result parses to no chunks") {
	const std::string body = R"({"data": {"resultType": "streams", "result": []}})";
	auto chunks = ParseStreamsResponse(body);
	CHECK_EQ(chunks.size(), size_t(0));
}

TEST_CASE("error body without data throws") {
	bool threw = false;
	try {
		ParseStreamsResponse("parse error: something went wrong");
	} catch (const std::exception &) {
		threw = true;
	}
	CHECK(threw);
}

TEST_CASE("metric resultType is rejected") {
	const std::string body = R"({"data": {"resultType": "matrix", "result": []}})";
	bool threw = false;
	try {
		ParseStreamsResponse(body);
	} catch (const std::exception &) {
		threw = true;
	}
	CHECK(threw);
}
