cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME loki)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)
find_package(yyjson CONFIG REQUIRED)

# cpp-httplib is header-only. vcpkg (used by CI / community-extensions) ships a CMake config
# exposing the httplib::httplib target; some system installs (e.g. Arch) ship only the header.
# Support both: prefer the config target, otherwise just locate the header directory.
find_package(httplib CONFIG QUIET)
if(NOT httplib_FOUND)
  find_path(HTTPLIB_INCLUDE_DIR httplib.h REQUIRED)
endif()

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD "17" CACHE STRING "C++ standard to enforce")
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(src/include)

set(EXTENSION_SOURCES
    src/loki_extension.cpp
    src/loki/http_request.cpp
    src/loki/parse.cpp
    src/loki/http_client.cpp
    src/loki/auth.cpp
    src/loki/pager.cpp
    src/loki/time_bounds.cpp
    src/loki/logql.cpp
    src/loki/secret.cpp
    src/loki/loki_scan.cpp)

# Only the HTTP client TU needs cpp-httplib's TLS path; scope the define to it so the rest
# of the build is unaffected.
set_source_files_properties(src/loki/http_client.cpp PROPERTIES COMPILE_DEFINITIONS CPPHTTPLIB_OPENSSL_SUPPORT)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL (TLS) and yyjson (JSON) into both the static library and the loadable extension.
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto yyjson::yyjson)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto yyjson::yyjson)

# cpp-httplib: link the config target when present (vcpkg), else add the located header dir.
if(httplib_FOUND)
  target_link_libraries(${EXTENSION_NAME} httplib::httplib)
  target_link_libraries(${LOADABLE_EXTENSION_NAME} httplib::httplib)
else()
  target_include_directories(${EXTENSION_NAME} PRIVATE ${HTTPLIB_INCLUDE_DIR})
  target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE ${HTTPLIB_INCLUDE_DIR})
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
