# DuckDB `loki` Extension — Design & Task Specification

**Status:** draft v0.1 · **Language:** C++ · **Target:** publishable DuckDB Community Extension

---

## 1. Vision

A DuckDB extension that turns a Grafana Loki instance into a first-class, queryable
data source. The user writes SQL (and, where they want precision, LogQL), joins log
data against anything else in DuckDB — Parquet, CSV, Postgres, other tables — and never
constructs an HTTP request by hand. The extension speaks Loki's language internally
(LogQL over the Loki HTTP API) and translates the parts of a SQL query that Loki can
execute efficiently into the REST call, leaving everything else to DuckDB.

The bar is a **high-quality, generally useful extension** that could land in the
official community extensions list: correct results, good ergonomics, secrets-based
auth, paging that handles real-world log volumes, projection and filter pushdown, tests,
and cross-platform CI.

---

## 2. Goals and non-goals

### Goals (v1.0)

- Query Loki **log streams** from SQL via a table function.
- **Filter pushdown**: translate SQL predicates on labels, timestamp, and log line into
  the LogQL stream selector, `start`/`end` time bounds, and line filters. Leave
  untranslatable predicates as residuals for DuckDB to apply.
- **Projection pushdown**: skip materializing columns (e.g. the labels map) the query
  doesn't select.
- A raw **LogQL passthrough** entry point (`loki_scan`) for power users.
- **Secrets-based** connection config (endpoint, auth, tenant) via DuckDB's Secret
  Manager, plus per-call overrides.
- **Paging** over Loki's per-request entry limit to return complete result sets.
- Helper functions for **label discovery** (`loki_labels`, `loki_label_values`,
  `loki_series`) so the source feels explorable and autocompletes well.
- Cross-platform builds (Linux/macOS/Windows × amd64/arm64) and a community-extensions
  submission.

### Non-goals (v1.0)

- **Writing/pushing logs** into Loki. Read-only for now.
- **Metric queries** (LogQL that returns `matrix`/`vector` — rates, counts, etc.).
  Different result shape; deferred to a stretch function.
- **Full LogQL parsing/validation.** We build selectors and pipeline stages; we do not
  re-implement Loki's parser. Passthrough LogQL is sent as-is.
- **`ATTACH` in v1.0.** The catalog integration is a deliberate **fast-follow (§3.6)**,
  not part of the first release — it's the largest, most version-coupled chunk of C++ and
  only pays off once the pushdown core is proven. What we *do* permanently reject is the
  **streams-or-label-values-as-tables** mapping (one table per `job`/label value): it
  explodes the catalog and hammers `/series` on every enumeration. The supported mapping is
  a single `logs` virtual table driven by `WHERE` pushdown (§3.6).

### Stretch (post-1.0)

- Metric queries via `loki_metric_query(...)` returning `(timestamp, value, labels)`.
- Parallel reads sharded across sub-time-ranges (`max_threads > 1`).
- Bind-time **automatic label-column discovery** via `/labels`.
- `tail`/live streaming, `patterns`/detected-fields endpoints.

---

## 3. User-facing design

### 3.1 Connection via Secret Manager

Follow the pattern `httpfs` uses. Store endpoint and credentials once:

```sql
CREATE SECRET my_loki (
    TYPE loki,
    ENDPOINT 'https://logs-prod.example.net',
    -- one of the auth styles:
    TOKEN 'glc_...'                       -- bearer / API token
    -- USERNAME 'tenant', PASSWORD 'xxx'  -- basic auth (e.g. Grafana Cloud)
    -- HEADERS MAP { 'X-Api-Key': '...' } -- arbitrary headers
    ,
    TENANT '1234'                         -- becomes X-Scope-OrgID (multi-tenant Loki)
);
```

Every table function resolves a secret by name (`secret := 'my_loki'`) or falls back to
the default `loki` secret. Individual connection fields may also be overridden inline for
ad-hoc use.

### 3.2 Primary table function — `loki(...)` (pushdown-driven)

```sql
SELECT timestamp, level, line
FROM loki(
    selector := '{job="api"}',      -- mandatory base stream selector (see §4.3)
    labels   := ['level','pod'],    -- labels promoted to top-level columns (see §4.2)
    secret   := 'my_loki'
)
WHERE level = 'error'                            -- → label matcher, pushed down
  AND timestamp >= now() - INTERVAL 2 HOUR       -- → start bound, pushed down
  AND line LIKE '%timeout%'                       -- → |= "timeout" line filter, pushed down
  AND length(line) > 200;                         -- → residual, applied by DuckDB
```

**Output columns:**

| column                | type                    | notes |
|-----------------------|-------------------------|-------|
| `timestamp`           | `TIMESTAMP_NS` (UTC)    | nanosecond precision preserved; see §6.5 |
| `line`                | `VARCHAR`               | raw log line |
| *one col per label in `labels`* | `VARCHAR`     | dynamic; enables label predicate pushdown |
| `labels`              | `MAP(VARCHAR, VARCHAR)` | all stream labels (skippable via projection pushdown) |
| `structured_metadata` | `MAP(VARCHAR, VARCHAR)` | per-entry structured metadata (newer Loki) |

### 3.3 Escape hatch — `loki_scan(...)` (raw LogQL)

For anyone who wants full LogQL control and no rewriting:

```sql
SELECT *
FROM loki_scan(
    query  := '{job="api"} | json | status >= 500 | line_format "{{.msg}}"',
    start  := now() - INTERVAL 1 DAY,
    end    := now(),
    limit  := 50000,
    secret := 'my_loki'
);
```

`loki_scan` sends `query` essentially verbatim. It still pages and still applies time
bounds, but does **not** attempt predicate translation — the user has already spoken
LogQL. This satisfies the demanding client while `loki()` serves the seamless-SQL case.

### 3.4 Discovery helpers

```sql
SELECT * FROM loki_labels(secret := 'my_loki');                 -- label names
SELECT * FROM loki_label_values('job', secret := 'my_loki');    -- values for a label
SELECT * FROM loki_series('{job="api"}', secret := 'my_loki');  -- matching series
```

### 3.5 The zero-cost "table" — a view over `loki()`

Before the catalog integration exists (and often instead of it), a plain view gives the
`FROM logs WHERE ...` experience with full pushdown and **no extra C++**:

```sql
CREATE VIEW logs AS
    SELECT * FROM loki(selector := '{job=~".+"}', labels := ['job','level']);

SELECT timestamp, line
FROM logs
WHERE job = 'api' AND level = 'error'          -- pushed through the view into loki()'s pushdown
  AND timestamp >= now() - INTERVAL 1 HOUR;
```

DuckDB pushes the view's predicates down into the underlying table function's
`pushdown_complex_filter`, so a permissive base selector (`{job=~".+"}` matches any stream
carrying a `job` label) gets refined by the `WHERE`. This is the pragmatic 80% of "Loki as
a table." What `ATTACH` (§3.6) adds on top is catalog/schema semantics, discovery and
autocomplete, multi-tenant schemas, and not having to hand-write the base selector.

### 3.6 `ATTACH` — Loki as a database (fast-follow, post-1.0)

**Goal:** make Loki appear as an attached database so it feels first-class:

```sql
ATTACH 'my_loki' AS loki (TYPE loki);         -- 'my_loki' resolves a secret
-- or inline:  ATTACH 'https://logs.example.net' AS loki (TYPE loki, TOKEN '...', TENANT '1234');

SELECT timestamp, line
FROM loki.public.logs
WHERE job = 'api' AND level = 'error'
  AND timestamp >= now() - INTERVAL 2 HOUR
  AND line LIKE '%timeout%';
```

**Catalog structure (the supported mapping):**

- **Database** = the attach alias (`loki`).
- **Schema** = tenant. Single-tenant Loki → one schema (`public`). Multi-tenant → one
  schema per tenant; because Loki has no API to enumerate tenants, the tenant list is
  supplied in the `ATTACH` options (or you attach once per tenant).
- **Tables** = a small **fixed** set, never per-stream:
  - `logs` — the main virtual table. Columns: `timestamp`, `line`, `labels` MAP,
    `structured_metadata` MAP, plus one `VARCHAR` column per label discovered via
    `/labels` at attach time (so label predicates are pushable per §4.2). Its scan is
    backed by the **same** bind / `pushdown_complex_filter` / paging / parser as `loki()`
    — the catalog layer is a thin adapter, not a second implementation.
  - `labels` *(optional, read-only)* — label names, for exploration/autocomplete.
  - `series` *(optional, read-only)* — backed by `/series`; requires a selector to avoid
    unbounded enumeration.

**The one caveat that keeps this from being 100% a database:** Loki refuses any query
without a stream selector, so a bare `SELECT * FROM loki.public.logs` (no `WHERE`) **cannot
run** — it must raise a clear error asking for at least one label filter. This is Loki's
model, not a limitation we can engineer away, and it means BI tools or preview queries that
assume a plain table scan will hit it. Document it prominently.

**Softer caveats:** discovered label columns are sparse (NULL where a stream lacks the
label) and can go stale as new labels appear — cache `/labels` with a TTL and a refresh
path (re-`ATTACH` or a `PRAGMA`). Catalog enumeration (autocomplete, `information_schema`)
must be served from that cache, never a live Loki call per lookup.

**Scope:** read-only. No `INSERT`/`CREATE`/`DROP` against Loki; the transaction manager is
a trivial read-only stub.

**Cost:** this is the largest, most DuckDB-version-coupled chunk of C++ in the project
(`StorageExtension`, `Catalog`, `SchemaCatalogEntry`, `TableCatalogEntry`, attach hooks).
Build it only after §4's pushdown core is solid; it reuses that core wholesale.

---

## 4. Filter pushdown — the core

This is the reason the extension is C++: DuckDB only exposes parsed `WHERE` filters to
table functions through the **C++** `TableFunction` API (via `pushdown_complex_filter` /
the per-expression `pushdown_expression` callback). The C Extension API used by
Rust/Go/Zig exposes projection pushdown only, not filters (as of DuckDB 1.5.x; there is
an open PR to add it, but it has not shipped). So the whole extension is C++ to unlock
this one capability.

### 4.1 Translation table

| SQL predicate (on a declared column)             | LogQL / API translation            |
|--------------------------------------------------|------------------------------------|
| `label = 'v'`                                    | selector matcher `{label="v"}`     |
| `label != 'v'`                                   | `{label!="v"}`                     |
| `label IN ('a','b')`                             | `{label=~"a|b"}`                   |
| `regexp_matches(label, 're')`                    | `{label=~"re"}`                    |
| `timestamp >= X` / `> X`                         | `start` bound                      |
| `timestamp <= Y` / `< Y`                         | `end` bound                        |
| `timestamp BETWEEN X AND Y`                      | `start` + `end`                    |
| `line LIKE '%sub%'`                              | line filter `|= "sub"`             |
| `line NOT LIKE '%sub%'`                          | `!= "sub"`                         |
| `regexp_matches(line, 're')`                     | `|~ "re"`                          |
| anything else (computed exprs, unknown funcs)    | **residual** — left in the plan    |

### 4.2 Why labels must be columns

DuckDB pushes filters on **declared columns**, not on expressions like `labels['job']`.
So to make `WHERE job = 'error'` pushable, `job` has to be a real output column. That's
what the `labels := [...]` parameter is for: each named label becomes a `VARCHAR` column,
and predicates on it are eligible for pushdown. Labels not listed still appear inside the
`labels` MAP but are not individually pushable in v1. (Auto-discovering label columns at
bind time via `/labels` is a stretch goal.)

### 4.3 The mandatory-selector rule

Loki refuses queries without at least one stream selector matcher that matches a
non-empty value. Therefore:

- `loki()` takes a base `selector` argument. Pushed-down label equalities **refine** it.
- If no `selector` is given and pushdown can synthesize at least one `=`/`=~` matcher
  from `WHERE`, use that.
- If neither exists, raise a clear error: a selector is required.

### 4.4 Correctness contract

**Never claim a filter you cannot fully honor.** Do not set the blanket
`filter_pushdown = true` flag — that tells DuckDB to remove *all* filters from the plan
and trust the function to apply every one. Instead use `pushdown_complex_filter`: walk
the expression list, **consume** (erase) only the predicates translated into LogQL, and
**return the rest** so DuckDB applies them after the scan. Loki does coarse filtering;
DuckDB finishes the job. Result correctness must never depend on Loki interpreting a
predicate we mistranslated — when in doubt, leave it residual.

Boundary detail to verify during implementation: Loki's `start` is inclusive and `end`
is exclusive on `query_range`; make the timestamp-bound translation match, or widen by
one nanosecond and let the residual filter tighten.

---

## 5. Loki API surface used

Base path: `{endpoint}/loki/api/v1`. All read endpoints; auth + `X-Scope-OrgID` applied
from the resolved secret.

| Endpoint                        | Used by            | Key params                                            |
|---------------------------------|--------------------|-------------------------------------------------------|
| `GET /query_range`              | `loki`, `loki_scan`| `query`, `start`, `end`, `limit`, `direction`, `step` |
| `GET /query`                    | instant queries    | `query`, `time`, `limit`, `direction`                 |
| `GET /labels`                   | `loki_labels`      | `start`, `end`                                        |
| `GET /label/{name}/values`      | `loki_label_values`| `start`, `end`, `query`                               |
| `GET /series`                   | `loki_series`      | `match[]`, `start`, `end`                             |

**Streams response shape** (`data.resultType == "streams"`):

```
data.result[] = {
  "stream": { "<label>": "<value>", ... },
  "values": [ [ "<ns_epoch_string>", "<line>" ], ... ],
  "structuredMetadata": ...   // when present
}
```

Each `values` entry is a `[timestamp_ns, line]` pair. Timestamps are strings holding
nanosecond Unix epoch.

**Operational constraints to respect:**

- `limit` per request is server-capped (commonly 5000). Larger user limits require
  paging (§6.4).
- Loki may reject very large time ranges or enforce query splitting; surface such errors
  clearly. Optional: pre-split large ranges client-side.
- Errors arrive as non-2xx with a JSON or text body — parse and surface the message.

---

## 6. Architecture / internals

### 6.1 Separation of concerns

Keep DuckDB-specific C++ as a thin shell around **pure, unit-testable functions**:

- `BuildLogQL(base_selector, pushed_matchers, line_filters) -> string`
- `BuildQueryRangeRequest(logql, start_ns, end_ns, limit, direction) -> HttpRequest`
- `ParseStreamsResponse(json_bytes) -> vector<StreamChunk>`
- `TranslateFilters(TableFilterSet / expressions, schema) -> {matchers, bounds, line_filters, residuals}`

The LogQL builder, request builder, response parser, and filter translator should all be
testable without a DuckDB or a Loki instance in the loop.

### 6.2 Table function lifecycle (C++)

- **bind** — parse arguments, resolve the secret, decide the output schema (fixed columns
  + one per requested label), stash `selector`/time defaults into `BindData`.
- **pushdown_complex_filter** — translate predicates (§4), mutate the LogQL/bounds held in
  bind data, erase consumed filters, return residuals.
- **init (global/local)** — set up paging cursor and HTTP client; honor projection
  pushdown (`column_ids`) so we only build requested columns.
- **function** — fetch the next page, decode, and fill up to `STANDARD_VECTOR_SIZE`
  (2048) rows per output chunk; stream rather than buffering the whole result.

### 6.3 Projection pushdown

Read the projected `column_ids` in init. If `labels` / `structured_metadata` maps aren't
requested, skip building them (they're the expensive columns). Only emit requested label
columns.

### 6.4 Paging

Loki caps entries per response. To return a complete set:

- Page by advancing the time cursor to the last-seen timestamp (direction-aware:
  `backward` moves `end` down, `forward` moves `start` up).
- De-duplicate at page boundaries — multiple entries can share an identical nanosecond
  timestamp, so track the last emitted `(ts, line, stream)` and drop repeats.
- Stop at the user's `limit` or when a page returns fewer than the requested entries.

### 6.5 Timestamp type

Default to `TIMESTAMP_NS` (UTC) to preserve Loki's nanosecond resolution. Offer a config
to downcast to microsecond `TIMESTAMP` for tools that expect it. Accept DuckDB
`TIMESTAMP`/`TIMESTAMPTZ`/`INTERVAL` expressions for `start`/`end` and convert to ns epoch
for the API.

### 6.6 Defaults

If `start`/`end` are omitted and none are pushed down, default to `end = now()`,
`start = now() - 1 hour`, and document it loudly. Never issue an unbounded query.

---

## 7. Tech stack & dependencies

- **Base:** fork `duckdb/extension-template` (CMake + vcpkg, the standard C++ scaffold;
  its GitHub Actions produce the cross-platform artifacts the community repo expects).
- **HTTP client:** `cpp-httplib` via vcpkg (same library `httpfs` uses), with **OpenSSL**
  for TLS. Alternatively reuse DuckDB's HTTP infrastructure if convenient.
- **JSON:** `yyjson` (fast, already the engine behind DuckDB's JSON extension) for parsing
  responses; pull via vcpkg or vendor it.
- **C++ standard:** match the extension template / DuckDB (currently C++11-compatible
  surface; confirm against the template).
- **Secrets:** DuckDB Secret Manager C++ API for the `loki` secret type.

Pin dependency versions in `vcpkg.json`. Keep the binary small — the C-API note about
17K binaries doesn't apply to C++ extensions, but avoid heavyweight deps.

### Prior art to crib from

- **Airport** (Arrow Flight extension) — a remote data source that implements
  `pushdown_complex_filter` translation into a backend query. Its shape maps onto Loki far
  better than the Postgres/SQLite scanners. Study its filter-translation and paging code.
- **httpfs** — HTTP client usage, TLS, and the Secret Manager integration pattern.
- **`src/include/duckdb/function/table_function.hpp`** — authoritative, version-tied
  signatures for `pushdown_complex_filter`, `pushdown_expression`, projection pushdown.
  Copy signatures from the checked-out DuckDB source, not from docs or memory; the C++
  extension API is recompiled per DuckDB release.

---

## 8. Testing strategy

- **Unit tests** (no network): LogQL builder, request builder, filter translator (assert
  which predicates become matchers/bounds/line-filters vs residuals), and response parser
  against captured JSON fixtures.
- **sqllogictest** (`test/sql/*.test`, from the template): end-to-end SQL against a mock
  HTTP server returning canned Loki payloads — deterministic, no real Loki.
- **Integration** (CI, optional job): `docker compose` with a real Loki + a log generator;
  push known lines, assert selector/time/line-filter pushdown returns exactly them.
- **Correctness fuzzing:** for a sampling of predicates, compare `loki()` (pushdown) vs
  `loki_scan()` (raw) results — they must match. This guards the pushdown contract (§4.4).

---

## 9. CI/CD, packaging, publishing

- Use the extension template's GitHub Actions to build for
  **Linux/macOS/Windows × amd64/arm64**.
- Ship a `description.yaml` (metadata: name, description, version, repo, maintainers) and
  submit a PR to **`duckdb/community-extensions`**; their pipeline builds and signs the
  artifacts. After acceptance, users get it via `INSTALL loki FROM community; LOAD loki;`.
- **License:** MIT (matches community-extension expectations and keeps it redistributable).
- **README:** quickstart, secret setup, `loki` vs `loki_scan`, the pushdown table, and a
  worked "join Loki with a Parquet file" example — the join story is the headline feature.
- Version with DuckDB compatibility clearly stated; the C++ API is version-tied, so
  document which DuckDB versions each release targets.

---

## 10. Roadmap

- **v0.1 — walking skeleton:** fork template; `loki_scan` (raw LogQL) → `query_range` →
  stream parse → `(timestamp, line, labels)`; single request, no paging; inline endpoint
  arg. Prove the round trip.
- **v0.2 — secrets + paging:** `loki` secret type; paging with de-dup; `start`/`end` from
  `TIMESTAMP`/`INTERVAL`.
- **v0.3 — projection + label columns:** projection pushdown; `labels := [...]` promotion;
  `structured_metadata`.
- **v0.4 — filter pushdown:** `pushdown_complex_filter` for label / time / line filters
  with residual handling; the correctness fuzz test.
- **v0.5 — discovery + polish:** `loki_labels`, `loki_label_values`, `loki_series`; error
  messages; README (document the §3.5 view trick for "logs as a table"); docker integration
  test.
- **v1.0 — publish:** cross-platform CI green; `description.yaml`; community-extensions PR.
- **v1.1 — `ATTACH` (§3.6):** `StorageExtension` + catalog exposing a single `logs` table
  per tenant-schema, backed by the v0.4 pushdown core. The big DB-feel upgrade, deliberately
  after the core is proven.
- **Stretch:** metric queries; parallel sharded reads; auto label-column discovery; tail;
  multi-tenant schemas and optional `labels`/`series` catalog tables.

---

## 11. Open questions / decisions

1. **Timestamp default type** — `TIMESTAMP_NS` vs `TIMESTAMP`? (Leaning ns to avoid silent
   precision loss.)
2. **`start`/`end` boundary** — confirm Loki's inclusive/exclusive semantics and match the
   pushdown translation exactly.
3. **`limit` semantics** — does the user's `limit` mean total rows returned, or per-request
   cap? (Proposed: total; paging is internal.)
4. **Line-filter pushdown scope** — which `LIKE` patterns translate to `|=` (substring)
   vs `|~` (regex)? Prefix/suffix patterns need regex; be conservative and leave ambiguous
   cases residual.
5. **Auth surface** — how many auth styles to support in v1 (bearer, basic, arbitrary
   headers)? Grafana Cloud uses basic auth (user = tenant id, password = token).
6. **Metric queries** — confirm out of scope for v1; sketch `loki_metric_query` shape for
   later so the schema doesn't paint us into a corner.
7. **`ATTACH` bare-scan behavior (§3.6)** — when `SELECT * FROM loki.logs` has no pushable
   selector, hard-error vs. apply a default permissive selector like `{job=~".+"}`? (Leaning
   hard-error with a message that names the fix, to avoid silently expensive full scans.)
8. **Label-column staleness** — cache TTL for `/labels`, and how to refresh (re-`ATTACH`,
   a `PRAGMA`, or time-based)?

---

## 12. One-line summary for the README

> **`loki`** — query Grafana Loki from DuckDB in plain SQL, with label/time/line-filter
> pushdown into LogQL, so you can join your logs against anything.
