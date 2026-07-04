#pragma once

// Minimal dependency-free test harness for the standalone loki unit-test binary.
// (DuckDB's Catch2 runner is unavailable to extensions — see CMakeLists.txt.)

#include <functional>
#include <iostream>
#include <string>
#include <vector>

struct TestCase {
	std::string name;
	std::function<void()> fn;
};

inline std::vector<TestCase> &Tests() {
	static std::vector<TestCase> tests;
	return tests;
}

inline int &Failures() {
	static int failures = 0;
	return failures;
}

struct TestRegistrar {
	TestRegistrar(const std::string &name, std::function<void()> fn) {
		Tests().push_back({name, std::move(fn)});
	}
};

#define TU_CONCAT2(a, b) a##b
#define TU_CONCAT(a, b)  TU_CONCAT2(a, b)

#define TEST_CASE(name)                                                                                                \
	static void TU_CONCAT(tu_fn_, __LINE__)();                                                                         \
	static TestRegistrar TU_CONCAT(tu_reg_, __LINE__)(name, TU_CONCAT(tu_fn_, __LINE__));                              \
	static void TU_CONCAT(tu_fn_, __LINE__)()

#define CHECK(cond)                                                                                                    \
	do {                                                                                                               \
		if (!(cond)) {                                                                                                 \
			std::cerr << "  FAIL: " << #cond << " @ " << __FILE__ << ":" << __LINE__ << "\n";                          \
			Failures()++;                                                                                              \
		}                                                                                                              \
	} while (0)

#define CHECK_FALSE(cond)                                                                                              \
	do {                                                                                                               \
		if ((cond)) {                                                                                                  \
			std::cerr << "  FAIL: !(" << #cond << ") @ " << __FILE__ << ":" << __LINE__ << "\n";                       \
			Failures()++;                                                                                              \
		}                                                                                                              \
	} while (0)

#define CHECK_EQ(a, b)                                                                                                 \
	do {                                                                                                               \
		auto tu_va = (a);                                                                                              \
		auto tu_vb = (b);                                                                                              \
		if (!(tu_va == tu_vb)) {                                                                                       \
			std::cerr << "  FAIL: " << #a << " == " << #b << " (got '" << tu_va << "' vs '" << tu_vb << "') @ "        \
			          << __FILE__ << ":" << __LINE__ << "\n";                                                          \
			Failures()++;                                                                                              \
		}                                                                                                              \
	} while (0)
