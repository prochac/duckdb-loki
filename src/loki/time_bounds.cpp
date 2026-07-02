#include "loki/time_bounds.hpp"

#include <stdexcept>

namespace duckdb {
namespace loki {

int64_t IntervalToNanos(int32_t months, int32_t days, int64_t micros) {
	if (months != 0) {
		throw std::invalid_argument("loki_scan: month/year INTERVAL bounds are calendar-dependent and not supported; "
		                            "pass an absolute TIMESTAMP (e.g. now() - INTERVAL 30 DAY) instead");
	}
	constexpr int64_t NANOS_PER_DAY = 86400LL * 1000LL * 1000LL * 1000LL;
	constexpr int64_t NANOS_PER_MICRO = 1000LL;
	return static_cast<int64_t>(days) * NANOS_PER_DAY + micros * NANOS_PER_MICRO;
}

} // namespace loki
} // namespace duckdb
