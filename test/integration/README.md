# Integration test — pushdown against a real Loki

An end-to-end check (DESIGN.md §8) that runs the built extension against a live Grafana Loki
instead of canned payloads. It proves the pushdown contract on the wire: seed known log lines,
then assert that `loki()`'s selector / label / time / line-filter pushdown returns **exactly**
those lines, and that `loki()` (pushdown) and `loki_scan()` (raw LogQL) agree.

Unlike the `test/sql/*.test` sqllogictests (which only bind/`DESCRIBE`, no network), this suite
issues real HTTP requests, so it needs Docker and is kept out of the default CI run.

## Files

- `docker-compose.yml` — single-node `grafana/loki` on `localhost:3100`.
- `seed.sh` — pushes 6 fixed log lines via `POST /loki/api/v1/push`, prints the base epoch.
- `run.sh` — orchestrates: start Loki, wait, seed, run the assertions, tear down.

## Run it locally

Build the extension first (`make`), then:

```sh
./test/integration/run.sh
```

Requires `docker` (with the compose plugin), `curl`, and `jq`. The script starts and stops the
Loki container itself.

Environment overrides:

| var         | default                     | purpose                                             |
|-------------|-----------------------------|-----------------------------------------------------|
| `DUCKDB`    | `build/release/duckdb`      | duckdb CLI to drive                                 |
| `INIT_SQL`  | *(empty)*                   | SQL run before every query, e.g. to `LOAD` a loadable extension build |
| `LOKI_URL`  | `http://localhost:3100`     | Loki base URL                                       |
| `KEEP_LOKI` | *(unset)*                   | if set, don't start/stop compose — the caller owns the Loki lifecycle |

## In CI

`.github/workflows/Integration.yml` runs this on manual `workflow_dispatch` (it builds DuckDB
from source, which is slow). It builds the extension, brings Loki up with the same compose file,
and runs `run.sh` with `KEEP_LOKI=1` so the workflow manages the container lifecycle.
