# Standalone unit tests over the pure `loki::` helpers (LogQL/request builders, auth-header
# builder, streams parser, and the paging/de-dup cursor). Self-contained: it does NOT pull in
# DuckDB, because the extension-ci-tools build hardcodes -DENABLE_UNITTEST_CPP_TESTS=FALSE and
# so cannot host C++ tests in DuckDB's `unittest` binary.
#
# Run from the repo root:
#   cmake -B build/unit -S test/cpp && cmake --build build/unit && ctest --test-dir build/unit --output-on-failure
cmake_minimum_required(VERSION 3.5)
project(loki_unit_tests CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(yyjson CONFIG REQUIRED)

# Repo root, so we can reach ../../src.
get_filename_component(REPO_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../.." ABSOLUTE)

enable_testing()

add_executable(loki_unit_tests
    test_main.cpp
    test_auth.cpp
    test_pager.cpp
    test_time_bounds.cpp
    test_parse.cpp
    test_logql.cpp
    test_discovery.cpp
    ${REPO_ROOT}/src/loki/http_request.cpp
    ${REPO_ROOT}/src/loki/parse.cpp
    ${REPO_ROOT}/src/loki/auth.cpp
    ${REPO_ROOT}/src/loki/pager.cpp
    ${REPO_ROOT}/src/loki/time_bounds.cpp
    ${REPO_ROOT}/src/loki/logql.cpp)

target_include_directories(loki_unit_tests PRIVATE ${REPO_ROOT}/src/include)
target_link_libraries(loki_unit_tests yyjson::yyjson)

add_test(NAME loki_unit_tests COMMAND loki_unit_tests)
