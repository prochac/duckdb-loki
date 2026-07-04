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
	CHECK_EQ(chunks[0].values[0].ts_ns, int64_t(1700000000000000001LL));
	CHECK_EQ(chunks[0].values[0].line, std::string("boom"));
	CHECK_EQ(chunks[0].values[1].ts_ns, int64_t(1700000000000000000LL));
	// No 3rd element in the pairs -> no structured metadata.
	CHECK_EQ(chunks[0].values[0].structured_metadata.size(), size_t(0));
}

TEST_CASE("parse structured metadata from the categorized third values element") {
	// Shape returned with the `categorize-labels` response flag: the 3rd element is an object
	// {"structuredMetadata": {...}, "parsed": {...}}, and {} for entries without any.
	const std::string body = R"({
	  "data": {
	    "resultType": "streams",
	    "result": [
	      {
	        "stream": {"job": "api"},
	        "values": [
	          ["1700000000000000002", "hi", {"structuredMetadata": {"trace_id": "abc", "span": "1"}}],
	          ["1700000000000000001", "bare"],
	          ["1700000000000000000", "empty", {}]
	        ]
	      }
	    ]
	  }
	})";

	auto chunks = ParseStreamsResponse(body);
	CHECK_EQ(chunks.size(), size_t(1));
	CHECK_EQ(chunks[0].values.size(), size_t(3));

	// Present: parsed into the metadata map.
	CHECK_EQ(chunks[0].values[0].structured_metadata.size(), size_t(2));
	CHECK_EQ(chunks[0].values[0].structured_metadata.at("trace_id"), std::string("abc"));
	CHECK_EQ(chunks[0].values[0].structured_metadata.at("span"), std::string("1"));

	// Absent (2-element entry): empty map.
	CHECK_EQ(chunks[0].values[1].structured_metadata.size(), size_t(0));

	// Present but empty object: empty map.
	CHECK_EQ(chunks[0].values[2].structured_metadata.size(), size_t(0));
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
