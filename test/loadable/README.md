# Loadable-binary smoke test

Every other test suite (`make test`, `test/mock`, `test/integration`) drives a DuckDB build with
the extension **statically linked in** (via `require loki`). None of them exercises the actual
**loadable artifact** (`build/release/extension/loki/loki.duckdb_extension`) or the unsigned-`LOAD`
path that real users take outside the signed community-extensions repo.

`run.sh` closes that gap: it `LOAD`s the shipped binary into a **stock** DuckDB CLI — one that does
*not* have loki compiled in — and asserts it loads unsigned and registers working functions
(all offline: function registration, bind/`DESCRIBE` schema, secret-type registration, and a
representative error path). It refuses to run if the target CLI already has loki linked, since then
it wouldn't be testing the artifact.

## Running

```sh
make                                    # build the loadable artifact first
DUCKDB=/path/to/stock/duckdb ./test/loadable/run.sh
```

The `DUCKDB` CLI must be a stock DuckDB matching the build's version (**v1.5.4**) — e.g. the
official release CLI, or a system install. Overrides: `DUCKDB` (stock CLI), `EXT` (artifact path).

In CI this runs in the **Extension smoke tests** workflow, which downloads the official DuckDB CLI
and points `DUCKDB` at it — the same binary end users `LOAD` into.
