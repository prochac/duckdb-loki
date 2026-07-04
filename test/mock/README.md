# Mock-HTTP sqllogictest

A deterministic, no-network end-to-end test (DESIGN.md §8): DuckDB SQL runs through the real
extension code path (HTTP request build → response parse → pushdown), but against a tiny
in-process Python mock of the Loki API instead of a live Loki. It's the automated guard for the
one thing unit tests and the Docker integration test can't cheaply cover: that `loki()`'s SQL
`WHERE` predicates are actually translated into the LogQL query the extension sends.

## Files

- **`mock_loki.py`** — stdlib-only HTTP server mocking Loki's read endpoints
  (`/query_range`, `/labels`, `/label/{name}/values`, `/series`). It routes `/query_range` on a
  marker token in the LogQL `query`:
  - `__echo__` → **request-echo mode**: returns a single entry whose `line` is the verbatim LogQL
    the extension built, with the request params/auth in structured metadata. This makes
    "what did the extension ask Loki?" queryable, so pushdown translation is asserted directly.
  - `__stream__` / `__empty__` / `__error__` → **fixture mode**: canned streams for the parser,
    projection / label-promotion, empty-result, and HTTP-error tests.
- **`run.sh`** — starts the mock on an ephemeral port, exports `LOKI_MOCK_URL`, runs
  `test/sql/loki_mock.test` through DuckDB's `unittest` runner, and tears the mock down.
- **`../sql/loki_mock.test`** — the sqllogictest itself. Gated by `require-env LOKI_MOCK_URL`, so
  a plain `make test` skips it; only `run.sh` sets that var and thus runs it.

## Running

```sh
make                 # build the extension + test runner first
./test/mock/run.sh   # start mock, run the test, stop mock
```

Needs `python3` (stdlib only — no pip). Overrides: `UNITTEST` (test-runner path), `PYTHON`.

## Not covered here

Paging: the pager's per-request cap equals Loki's server cap (5000), so triggering real paging
needs 5000+ entries. That's covered end-to-end by the Docker integration test
(`test/integration/`) against a real Loki instead.
