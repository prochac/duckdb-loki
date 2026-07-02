#pragma once

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

// Register the `loki` secret type (DESIGN.md §3.1) so users can persist an endpoint and
// credentials once with `CREATE SECRET ... (TYPE loki, ENDPOINT ..., TOKEN ..., ...)` and
// resolve them by name from the table functions.
void RegisterLokiSecretType(ExtensionLoader &loader);

} // namespace duckdb
