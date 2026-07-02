# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

A DuckDB **C++ extension**, scaffolded from `duckdb/extension-template`. The end goal is
`loki` — a table-function-based extension that queries Grafana Loki from SQL with
label/time/line-filter pushdown into LogQL. **`DESIGN.md` is the authoritative spec and
roadmap** (vision, LogQL translation table, pushdown contract, table-function lifecycle,
versioned rollout v0.1→v1.1). Read it before implementing anything Loki-related.

**Current state:** the extension has been renamed from the template's `waddle` to `loki`
(via `scripts/bootstrap-template.py`, now deleted). The source (`src/loki_extension.cpp`) is
still just the template's placeholder — two scalar functions, `loki()` and
`loki_openssl_version()` — kept only to prove the build/test round-trip. **None of the Loki
design in DESIGN.md is implemented yet.** The next work is to replace the placeholder scalar
functions with the `loki()` / `loki_scan()` table functions and discovery helpers per
DESIGN.md.

## Build & test

```sh
make                 # build DuckDB + extension (release). First build compiles DuckDB — slow.
GEN=ninja make       # much faster rebuilds; requires ninja + ccache (recommended)
make test            # run sqllogictest suite in test/sql/*.test
make clean
```

Build outputs:
- `./build/release/duckdb` — shell with the extension preloaded
- `./build/release/test/unittest` — DuckDB test runner (extension linked in)
- `./build/release/extension/<name>/<name>.duckdb_extension` — loadable binary

Run a single sqllogictest file:
```sh
./build/release/test/unittest --test-dir . test/sql/loki.test
```
Filter DuckDB C++ unit tests by tag, e.g. `[sql]`:
```sh
./build/release/test/unittest "[sql]"
```

Requires submodules (`git submodule update --init --recursive`) — `duckdb/` and
`extension-ci-tools/` are pinned and provide the build system. DuckDB target version is
**v1.5.4** (see the workflow files); the C++ extension API is version-tied, so signatures
must be copied from the checked-out `duckdb/` source, not from docs/memory.

## Dependencies (vcpkg)

Deps go in `vcpkg.json` and are consumed via `find_package` in `CMakeLists.txt` (currently
just `openssl`). Anything added there must be linked into **both** targets
(`${EXTENSION_NAME}` static + `${LOADABLE_EXTENSION_NAME}` loadable). DESIGN.md §7 plans to
add an HTTP client (`cpp-httplib`) and JSON (`yyjson`) this way.

## Architecture notes for the Loki work

- Extensions register via the newer **`ExtensionLoader`** API and the
  `DUCKDB_CPP_EXTENSION_ENTRY(<name>, loader)` macro (see `src/loki_extension.cpp`), not the
  older `DatabaseInstance` entry point.
- Filter pushdown is the whole reason this is C++: only DuckDB's C++ `TableFunction` API
  exposes parsed `WHERE` filters (`pushdown_complex_filter`). Per DESIGN.md §4.4, **never** set
  the blanket `filter_pushdown = true` — walk the expression list, consume only predicates
  translated into LogQL, and return the rest as residuals for DuckDB to apply.
- Keep DuckDB-specific glue as a thin shell around pure, unit-testable functions (LogQL
  builder, request builder, streams-response parser, filter translator) — DESIGN.md §6.1.
- Tests: sqllogictest in `test/sql/*.test` against canned/mock Loki payloads (no live Loki),
  plus unit tests over the pure functions.

## Code quality

CI runs clang-format + clang-tidy (`.clang-format`/`.clang-tidy` symlink into `duckdb/`).
Match DuckDB's style. Cross-platform release builds (Linux/macOS/Windows × amd64/arm64) and
the format/tidy checks run via `.github/workflows/MainDistributionPipeline.yml`.
