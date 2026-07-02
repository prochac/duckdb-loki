#include "test_util.hpp"

int main() {
	for (auto &test : Tests()) {
		std::cerr << "[ RUN ] " << test.name << "\n";
		test.fn();
	}
	if (Failures() > 0) {
		std::cerr << Failures() << " check(s) failed across " << Tests().size() << " test(s)\n";
		return 1;
	}
	std::cerr << "All " << Tests().size() << " test(s) passed\n";
	return 0;
}
