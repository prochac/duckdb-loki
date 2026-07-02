#define DUCKDB_EXTENSION_MAIN

#include "loki_extension.hpp"
#include "loki/loki_scan.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void LokiScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "...........🦆 " + name.GetString());
	});
}

inline void LokiOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Loki " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto loki_scalar_function =
	    ScalarFunction("loki", {LogicalType::VARCHAR}, LogicalType::VARCHAR, LokiScalarFun);

	loader.RegisterFunction(loki_scalar_function);

	// Register another scalar function
	auto loki_openssl_version_scalar_function = ScalarFunction("loki_openssl_version", {LogicalType::VARCHAR},
	                                                             LogicalType::VARCHAR, LokiOpenSSLVersionScalarFun);
	loader.RegisterFunction(loki_openssl_version_scalar_function);

	// The real work: the loki_scan(...) table function (DESIGN.md §3.3, roadmap v0.1).
	RegisterLokiScanFunction(loader);
}

void LokiExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string LokiExtension::Name() {
	return "loki";
}

std::string LokiExtension::Version() const {
#ifdef EXT_VERSION_LOKI
	return EXT_VERSION_LOKI;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(loki, loader) {
	duckdb::LoadInternal(loader);
}
}
