# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(loki
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)
# DuckDB 2.0 builds an extension without linking it into the shell; link it so
# build/release/duckdb has loki loaded, as it did on 1.5.
duckdb_extension_statically_link(loki)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)