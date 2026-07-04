#!/usr/bin/env bash
#
# End-to-end integration test (DESIGN.md §8): spin up a real Loki, push known log lines, and
# assert that selector / label / time / line-filter pushdown through loki() returns exactly those
# lines — plus a correctness check that loki() (pushdown) and loki_scan() (raw LogQL) agree.
#
# Requires: docker (compose), curl, jq, and a built extension. By default it drives the statically
# linked shell at build/release/duckdb. Override with env vars:
#   DUCKDB      path to the duckdb CLI              (default: <repo>/build/release/duckdb)
#   INIT_SQL    SQL run before every query          (e.g. "SET allow_unsigned_extensions=true;
#                                                    LOAD 'path/loki.duckdb_extension';")
#   LOKI_URL    Loki base URL                        (default: http://localhost:3100)
#   KEEP_LOKI   if set, don't start/stop compose     (caller manages the Loki lifecycle)
#
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
DUCKDB="${DUCKDB:-$REPO_ROOT/build/release/duckdb}"
LOKI_URL="${LOKI_URL:-http://localhost:3100}"
INIT_SQL="${INIT_SQL:-}"
COMPOSE=(docker compose -f "$HERE/docker-compose.yml")

if [[ ! -x "$DUCKDB" ]]; then
	echo "duckdb binary not found at $DUCKDB — run 'make' first (or set DUCKDB=...)." >&2
	exit 1
fi

# Bring Loki up unless the caller manages it (KEEP_LOKI, e.g. in CI where a service is already up).
if [[ -z "${KEEP_LOKI:-}" ]]; then
	trap '"${COMPOSE[@]}" down -v >/dev/null 2>&1 || true' EXIT
	echo "==> Starting Loki"
	"${COMPOSE[@]}" up -d
fi

echo "==> Waiting for Loki to be ready"
ready=
for _ in $(seq 1 60); do
	if curl -sf "$LOKI_URL/ready" >/dev/null 2>&1; then ready=1; break; fi
	sleep 1
done
[[ -n "$ready" ]] || { echo "Loki did not become ready at $LOKI_URL" >&2; exit 1; }

echo "==> Seeding log data"
BASE="$(bash "$HERE/seed.sh" "$LOKI_URL")"
sleep 2 # give the ingester a moment to make the head chunk queryable

# UTC timestamp literals for query windows. DuckDB reads a bare TIMESTAMP as the same wall-clock
# instant the extension sends to Loki as a ns epoch, so UTC on both sides keeps them aligned.
iso() { date -u -d "@$1" '+%Y-%m-%d %H:%M:%S'; }
WIN_START="$(iso $((BASE - 60)))"
WIN_END="$(iso $((BASE + 60)))"
T2="$(iso $((BASE + 2)))"
T4="$(iso $((BASE + 4)))"

# A wide window covering all seeded entries, shared by most queries. `end` is a reserved word,
# so the named parameter is quoted as an identifier.
WIN="endpoint := '$LOKI_URL', start := TIMESTAMP '$WIN_START', \"end\" := TIMESTAMP '$WIN_END'"

fails=0
# assert <description> <expected> <sql>
assert() {
	local desc="$1" expected="$2" sql="$3" got
	if ! got="$("$DUCKDB" -noheader -list -c "$INIT_SQL$sql" 2>/tmp/loki_it_err)"; then
		echo "FAIL: $desc (query error)"; sed 's/^/      /' /tmp/loki_it_err; fails=$((fails + 1)); return
	fi
	if [[ "$got" == "$expected" ]]; then
		echo "PASS: $desc"
	else
		echo "FAIL: $desc — expected [$expected], got [$got]"; fails=$((fails + 1))
	fi
}

echo "==> Running assertions"

# 1. Base selector pushdown: {job="api"} returns exactly the 4 api entries.
assert "selector {job=api} → 4 rows" 4 \
	"SELECT count(*) FROM loki(selector := '{job=\"api\"}', $WIN);"

# 2. Label-equality pushdown: level='error' refines the selector to the 2 error entries.
assert "label level=error → 2 rows" 2 \
	"SELECT count(*) FROM loki(selector := '{job=\"api\"}', labels := ['level'], $WIN) WHERE level = 'error';"

# 3. Line-filter pushdown inside the api selector: only 'connection timeout ...' matches.
assert "line LIKE %timeout% within api → 1 row" 1 \
	"SELECT count(*) FROM loki(selector := '{job=\"api\"}', $WIN) WHERE line LIKE '%timeout%';"

# 4. The same line filter across all jobs also catches the web warning line.
assert "line LIKE %timeout% across all jobs → 2 rows" 2 \
	"SELECT count(*) FROM loki(selector := '{job=~\".+\"}', $WIN) WHERE line LIKE '%timeout%';"

# 5. Line filter returns the exact expected content, not just a count.
assert "line filter returns exact api line" "connection timeout to upstream" \
	"SELECT line FROM loki(selector := '{job=\"api\"}', $WIN) WHERE line LIKE '%timeout%';"

# 6. Time-bound pushdown: window [base+2, base+4) keeps exactly the two error entries.
assert "time bound [T2,T4) → 2 rows" 2 \
	"SELECT count(*) FROM loki(selector := '{job=\"api\"}', endpoint := '$LOKI_URL', start := TIMESTAMP '$T2', \"end\" := TIMESTAMP '$T4') WHERE timestamp >= TIMESTAMP '$T2' AND timestamp < TIMESTAMP '$T4';"

# 7. Correctness contract (§4.4 / §8): loki() pushdown and loki_scan() raw LogQL must agree.
#    The symmetric difference is empty AND the result is non-empty → the expression evaluates to 0.
assert "loki() pushdown == loki_scan() raw" 0 "
WITH a AS (
  SELECT timestamp, line FROM loki(selector := '{job=\"api\"}', labels := ['level'], $WIN)
    WHERE level = 'error' AND line LIKE '%timeout%'
), b AS (
  SELECT timestamp, line FROM loki_scan('{job=\"api\",level=\"error\"} |= \"timeout\"', $WIN)
)
SELECT (SELECT count(*) FROM (SELECT * FROM a EXCEPT SELECT * FROM b))
     + (SELECT count(*) FROM (SELECT * FROM b EXCEPT SELECT * FROM a))
     + CASE WHEN (SELECT count(*) FROM a) = 0 THEN 1 ELSE 0 END;"

echo "==> Done"
if [[ "$fails" -gt 0 ]]; then
	echo "$fails assertion(s) failed." >&2
	exit 1
fi
echo "All integration assertions passed."
