# loki

**`loki`** — query [Grafana Loki](https://grafana.com/oss/loki/) from DuckDB in plain SQL,
with label/time/line-filter pushdown into LogQL, so you can join your logs against anything.

This repository is based on https://github.com/duckdb/extension-template.

> **Status:** preparing the **v1.0** publish. Working end-to-end: the raw-LogQL
> `loki_scan()` function, the pushdown-driven `loki()` function (label/time/line-filter
> pushdown into LogQL), promoted **label columns**, **projection pushdown**,
> **structured metadata**, the `loki_labels` / `loki_label_values` / `loki_series` discovery
> helpers, **secrets-based auth**, and **automatic paging** over Loki's per-request cap.
> Targets DuckDB **v1.5.4**. Still to come: the community-extensions submission (v1.0) and
> `ATTACH` (v1.1). See [`DESIGN.md`](./DESIGN.md) for the full specification and roadmap.

## Usage

There are two log-query table functions:

- **`loki_scan(query, ...)`** — send a raw [LogQL](https://grafana.com/docs/loki/latest/query/)
  query through essentially verbatim. Full LogQL control, no predicate translation.
- **`loki(selector := ..., ...)`** — a base stream selector that SQL `WHERE` predicates *refine*:
  filters on promoted label columns, `timestamp`, and `line` are translated into the LogQL
  selector, time bounds, and line filters (everything else is applied by DuckDB). See the
  **Pushdown** section below.

Both return one row per log entry with the same schema:

| column                | type                    | notes                                       |
|-----------------------|-------------------------|---------------------------------------------|
| `timestamp`           | `TIMESTAMP_NS` (UTC)    | nanosecond precision preserved              |
| `line`                | `VARCHAR`               | the raw log line                            |
| *one per promoted label* | `VARCHAR`            | from `labels := [...]`; NULL where missing  |
| `labels`              | `MAP(VARCHAR, VARCHAR)` | the stream's labels                         |
| `structured_metadata` | `MAP(VARCHAR, VARCHAR)` | per-entry structured metadata (newer Loki)  |

```sql
LOAD loki;

SELECT timestamp, line, labels
FROM loki_scan('{job="api"} |= `timeout`', endpoint := 'http://localhost:3100')
ORDER BY timestamp;
```

Parameters:
- `query` *(`loki_scan` only, required, positional)* — the LogQL query. Sent to Loki
  essentially verbatim, so a stream selector (e.g. `{job="api"}`) is mandatory — Loki rejects
  queries without one.
- `selector` *(`loki` only)* — the base stream selector that `WHERE` predicates refine. May be
  omitted if a `WHERE` equality/IN on a promoted label can synthesize one; otherwise required.
- `labels` *(optional)* — a `LIST` of label names to promote to top-level `VARCHAR` columns
  (e.g. `labels := ['job','level']`). For `loki()` this is what makes a label's `WHERE`
  predicates pushable (§4.2); unlisted labels still appear inside the `labels` MAP.
- `endpoint` — `scheme://host[:port]` of the Loki instance. `https://` is supported (TLS via
  OpenSSL). Required unless a secret provides it (see below).
- `secret` — name of a `loki` secret to resolve connection + auth from. Falls back to a secret
  named `loki` if present.
- `token` / `username` / `password` / `tenant` / `headers` — inline auth overrides (a bearer
  token, basic-auth credentials, the `X-Scope-OrgID` tenant, or an arbitrary header `MAP`).
  These take precedence over the secret.
- `start` / `end` *(optional)* — time bounds as `TIMESTAMP`/`TIMESTAMPTZ` (absolute) or
  `INTERVAL` (an offset added to `now()`, so `-INTERVAL 2 HOUR` means two hours ago). Default
  to the last hour ending now; a query is never issued unbounded.
- `limit` *(optional)* — **total** number of entries returned across all pages (default 5000).
  The extension pages internally over Loki's per-request cap and de-duplicates page boundaries.
- `direction` *(optional)* — `'backward'` (newest first, default) or `'forward'`.

### Secrets

Store an endpoint and credentials once with DuckDB's Secret Manager:

```sql
CREATE SECRET my_loki (
    TYPE loki,
    ENDPOINT 'https://logs-prod.example.net',
    TOKEN 'glc_...',        -- bearer token; or USERNAME + PASSWORD for basic auth
    TENANT '1234'           -- becomes X-Scope-OrgID for multi-tenant Loki
);

SELECT timestamp, line
FROM loki_scan('{job="api"}', secret := 'my_loki', "limit" := 50000, start := -INTERVAL 1 DAY)
ORDER BY timestamp;
```

A secret named `loki` is used automatically when `secret :=` is omitted.

> **Caveats (v0.2):**
> - `end` and `limit` are SQL reserved words, so they must be **double-quoted** when used as
>   named parameters: `"end" := now()`, `"limit" := 1000`. (`start` needs no quoting.)
> - `INTERVAL` bounds are computed against `now()` in C++ (no ICU needed). Calendar-dependent
>   month/year intervals are rejected — pass an absolute `TIMESTAMP` for those. Relative bounds
>   written as `now() - INTERVAL 2 HOUR` still require the built-in **ICU** extension
>   (`INSTALL icu; LOAD icu;`) because that arithmetic happens in SQL, not in the extension.
> - Only log queries are supported; metric LogQL (rates/counts, which return a `matrix`/
>   `vector`) raises a clear error.

## Pushdown — `loki()`

`loki()` takes a base `selector` and lets DuckDB's `WHERE` clause refine it. Predicates that
Loki can execute are translated and pushed down; everything else is left for DuckDB to apply
after the scan, so results are always correct.

```sql
SELECT timestamp, level, line
FROM loki(
    selector := '{job="api"}',
    labels   := ['level','pod'],       -- promote these labels to columns so they're pushable
    endpoint := 'http://localhost:3100'
)
WHERE level = 'error'                          -- → selector matcher {level="error"}
  AND timestamp >= TIMESTAMP '2026-07-04 00:00:00'  -- → start bound
  AND line LIKE '%timeout%'                    -- → line filter |= "timeout"
  AND length(line) > 200;                      -- → residual, applied by DuckDB
```

| SQL predicate (on a declared column) | pushed to Loki as        |
|--------------------------------------|--------------------------|
| `label = 'v'`                        | `{label="v"}`            |
| `label IN ('a','b')`                 | `{label=~"a\|b"}`        |
| `timestamp >= X` / `<= Y`            | `start` / `end` bound    |
| `line LIKE '%sub%'`                  | `\|= "sub"` line filter  |
| `regexp_matches(line, 're')`         | `\|~ "re"` line filter   |
| anything else                        | residual (DuckDB applies)|

Loki refuses a query with no stream selector, so `loki()` needs either a `selector` argument or
a `WHERE` equality/IN on a promoted label to synthesize one — otherwise it raises a clear error.

### Loki as a table — a view over `loki()`

Because DuckDB pushes a view's predicates down into the underlying table function, a plain view
over `loki()` gives the `FROM logs WHERE ...` experience with full pushdown and no extra setup:

```sql
CREATE VIEW logs AS
    SELECT * FROM loki(selector := '{job=~".+"}', labels := ['job','level']);

SELECT timestamp, line
FROM logs
WHERE job = 'api' AND level = 'error'          -- pushed through the view into loki()'s pushdown
  AND timestamp >= TIMESTAMP '2026-07-04 00:00:00';
```

## Discovery helpers

Explore what's in Loki without hand-writing API calls:

```sql
SELECT * FROM loki_labels();                    -- all label names → column `label`
SELECT * FROM loki_label_values('job');         -- values for one label → column `value`
SELECT * FROM loki_series('{job="api"}');       -- matching series → column `labels` (MAP)
```

- `loki_label_values(name, ...)` accepts an optional `query := '{job="api"}'` selector to
  restrict which series contribute values.
- `loki_series(match, ...)` requires a selector — Loki rejects `/series` without one.
- All three resolve a `loki` secret (with inline overrides) like the scan functions, and accept
  optional `start` / `end` bounds. When omitted, Loki applies its own default window.

## Building

### Managing dependencies
DuckDB extensions use VCPKG for dependency management. Enable it by following the
[installation instructions](https://vcpkg.io/en/getting-started) or running:
```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```

### Build steps
```sh
make
```
The main binaries built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/loki/loki.duckdb_extension
```
- `duckdb` is the DuckDB shell with the extension automatically loaded.
- `unittest` is DuckDB's test runner, with the extension linked in.
- `loki.duckdb_extension` is the loadable binary as it would be distributed.

For faster rebuilds, install [ccache](https://ccache.dev/) and
[ninja](https://ninja-build.org/) and build with `GEN=ninja make`.

## Running the tests
The primary tests are the SQL tests in `./test/sql`, run with:
```sh
make test
```

The pure helpers (LogQL/request builders, auth-header builder, streams parser, paging/de-dup
cursor) also have standalone C++ unit tests that don't need DuckDB or a live Loki:
```sh
cmake -B build/unit -S test/cpp && cmake --build build/unit
ctest --test-dir build/unit --output-on-failure
```

For end-to-end SQL without a live Loki, `./test/mock` runs the SQL tests against a tiny
in-process Python mock of the Loki API. It's deterministic (no network) and, via a
request-echo mode, asserts exactly which `WHERE` predicates get translated into the LogQL
query — the guard for filter-pushdown *translation*. Needs only `python3`:
```sh
./test/mock/run.sh
```

All of the above drive a build with the extension **statically linked in**. To smoke-test the
actual shipped **loadable binary**, `./test/loadable` `LOAD`s it into a *stock* DuckDB CLI (one
without loki compiled in) and checks it loads unsigned and registers working functions — the path
real users take outside the signed community repo. Needs a stock `duckdb` CLI matching the build's
version:
```sh
DUCKDB=/path/to/stock/duckdb ./test/loadable/run.sh
```

There is also an end-to-end integration test in `./test/integration` that runs the built
extension against a real Grafana Loki in Docker — it seeds known log lines and asserts that
selector/label/time/line-filter pushdown returns exactly them. It needs `docker`, `curl`, and
`jq`:
```sh
./test/integration/run.sh
```
See [`test/integration/README.md`](test/integration/README.md) for details. It also runs in CI
on demand via the **Integration (Docker Loki)** workflow.

### Installing from community-extensions

Once the [community-extensions](https://github.com/duckdb/community-extensions) submission is
accepted, the canonical install is a signed one-liner (no `-unsigned` needed):

```sql
INSTALL loki FROM community;
LOAD loki;
```

### Installing the deployed binaries
To install extension binaries from a custom repository, launch DuckDB with
`allow_unsigned_extensions` set to true.

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Then set the repository endpoint to the HTTP URL of your bucket + extension version:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/loki/latest';
```
The `/latest` path installs the latest version available for your DuckDB version. To pin a
version, pass it instead of `latest`.

After these steps, install and load with the regular commands:
```sql
INSTALL loki;
LOAD loki;
```

## Setting up CLion

### Opening the project
Make sure the DuckDB submodule is available, then open `./duckdb/CMakeLists.txt` (not the
top-level `CMakeLists.txt`) as a project in CLion. Fix the project path via
`tools -> CMake -> Change Project Root`
([docs](https://www.jetbrains.com/help/clion/change-project-root-directory.html)) to the
repo root.

### Debugging
In `CLion -> Settings -> Build, Execution, Deploy -> CMake`, add the desired builds (Debug,
Release, etc.). Leave fields empty except `build path` (`../build/{build type}`) and add to
CMake Options:
```
-DDUCKDB_EXTENSION_CONFIGS=<path_to_the_extension_CMakeLists.txt>
```
Then configure the `unittest` runner as a CMake Application run configuration. To run only
the extension's tests, add `--test-dir ../../.. [sql]` to the Program Arguments.
