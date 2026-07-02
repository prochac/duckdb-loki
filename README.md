# loki

**`loki`** — query [Grafana Loki](https://grafana.com/oss/loki/) from DuckDB in plain SQL,
with label/time/line-filter pushdown into LogQL, so you can join your logs against anything.

This repository is based on https://github.com/duckdb/extension-template.

> **Status:** early development (roadmap **v0.1**, the walking skeleton). The raw-LogQL
> `loki_scan()` table function works end-to-end; secrets, paging, projection/filter pushdown,
> label columns, and discovery helpers are not implemented yet. See [`DESIGN.md`](./DESIGN.md)
> for the full specification and roadmap.

## Usage

`loki_scan(query, endpoint := ..., start := ..., end := ..., limit := ...)` runs a raw
[LogQL](https://grafana.com/docs/loki/latest/query/) log query against Loki's `query_range`
endpoint and returns one row per log entry:

| column      | type                    | notes                                  |
|-------------|-------------------------|----------------------------------------|
| `timestamp` | `TIMESTAMP_NS` (UTC)    | nanosecond precision preserved         |
| `line`      | `VARCHAR`               | the raw log line                       |
| `labels`    | `MAP(VARCHAR, VARCHAR)` | the stream's labels                    |

```sql
LOAD loki;

SELECT timestamp, line, labels
FROM loki_scan('{job="api"} |= `timeout`', endpoint := 'http://localhost:3100')
ORDER BY timestamp;
```

Parameters:
- `query` *(required, positional)* — the LogQL query. Sent to Loki essentially verbatim, so a
  stream selector (e.g. `{job="api"}`) is mandatory — Loki rejects queries without one.
- `endpoint` *(required)* — `scheme://host[:port]` of the Loki instance. `https://` is
  supported (TLS via OpenSSL). Secret-based configuration is planned for a later version.
- `start` / `end` *(optional)* — time bounds as `TIMESTAMP`/`TIMESTAMPTZ`. Default to the last
  hour ending now; a query is never issued unbounded.
- `limit` *(optional)* — max entries fetched in the single request (default 5000). Paging over
  larger result sets is planned for a later version.

> **Caveats (v0.1):**
> - `end` and `limit` are SQL reserved words, so they must be **double-quoted** when used as
>   named parameters: `"end" := now()`, `"limit" := 1000`. (`start` needs no quoting.)
> - Relative bounds like `now() - INTERVAL 2 HOUR` require the built-in **ICU** extension for
>   time-zone-aware timestamp arithmetic (`INSTALL icu; LOAD icu;`). Plain `TIMESTAMP` literals
>   work without it.
> - Only log queries are supported; metric LogQL (rates/counts, which return a `matrix`/
>   `vector`) raises a clear error.

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
