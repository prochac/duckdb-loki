#define DUCKDB_EXTENSION_MAIN

#include "loki_extension.hpp"
#include "loki/loki_scan.hpp"
#include "loki/secret.hpp"
#include "duckdb.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// The `loki` secret type (DESIGN.md §3.1, roadmap v0.2) — must register before the table
	// functions that resolve it.
	RegisterLokiSecretType(loader);

	// The table functions: loki_scan(...) (raw LogQL, DESIGN.md §3.3) and loki(...) (pushdown-
	// driven, DESIGN.md §3.2 / §4). The template's placeholder scalar functions were dropped
	// once the real loki() table function landed.
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
