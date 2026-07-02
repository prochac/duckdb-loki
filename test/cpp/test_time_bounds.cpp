#include "test_util.hpp"

#include "loki/time_bounds.hpp"

#include <stdexcept>

using duckdb::loki::IntervalToNanos;

TEST_CASE("interval micros to nanos") {
	CHECK_EQ(IntervalToNanos(0, 0, 0), int64_t(0));
	CHECK_EQ(IntervalToNanos(0, 0, 1), int64_t(1000));               // 1 micro = 1000 ns
	CHECK_EQ(IntervalToNanos(0, 0, 1000000), int64_t(1000000000LL)); // 1 s
}

TEST_CASE("interval days to nanos") {
	CHECK_EQ(IntervalToNanos(0, 1, 0), int64_t(86400LL * 1000000000LL));
	CHECK_EQ(IntervalToNanos(0, 2, 500000), int64_t(2LL * 86400LL * 1000000000LL + 500000LL * 1000LL));
}

TEST_CASE("negative interval offsets into the past") {
	CHECK_EQ(IntervalToNanos(0, 0, -1000000), int64_t(-1000000000LL));
}

TEST_CASE("month or year interval is rejected") {
	bool threw = false;
	try {
		IntervalToNanos(1, 0, 0);
	} catch (const std::invalid_argument &) {
		threw = true;
	}
	CHECK(threw);
}
