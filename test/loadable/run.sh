#!/usr/bin/env bash
#
# Loadable-binary smoke test: `LOAD` the *shipped* extension artifact
# (build/release/extension/loki/loki.duckdb_extension) into a STOCK DuckDB — one that does NOT
# have the extension statically linked — and check that it loads unsigned and registers working
# functions. The other test suites (make test, test/mock, test/integration) all drive a build with
# the extension compiled in via `require loki`, so none of them exercise the actual loadable
# artifact or the unsigned-LOAD path this one covers. All assertions here are offline (bind /
# DESCRIBE / error paths — no network, no Loki).
#
# Requires: a stock duckdb CLI matching the build's DuckDB version (v1.5.4), and a built extension.
#   DUCKDB   path to a STOCK duckdb CLI (no loki linked)   (default: `duckdb` on PATH)
#   EXT      path to the loadable extension                (default: <repo>/build/release/…)
#
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
DUCKDB="${DUCKDB:-duckdb}"
EXT="${EXT:-$REPO_ROOT/build/release/extension/loki/loki.duckdb_extension}"

command -v "$DUCKDB" >/dev/null 2>&1 || { echo "stock duckdb CLI '$DUCKDB' not found (set DUCKDB=...)." >&2; exit 1; }
[[ -f "$EXT" ]] || { echo "loadable extension not found at $EXT — run 'make' first (or set EXT=...)." >&2; exit 1; }

# The whole point is to load the artifact, so the CLI must not already carry loki statically.
if [[ "$("$DUCKDB" -noheader -list -c "SELECT count(*) FROM duckdb_functions() WHERE function_name = 'loki_scan';" 2>/dev/null)" != "0" ]]; then
	echo "ERROR: '$DUCKDB' already has the loki extension linked in — point DUCKDB at a stock duckdb CLI." >&2
	exit 1
fi

echo "==> DuckDB:    $("$DUCKDB" --version)"
echo "==> Extension: $EXT ($(du -h "$EXT" | cut -f1))"

PRELUDE="LOAD '$EXT';"
fails=0

# assert <description> <expected> <sql>
assert() {
	local desc="$1" expected="$2" sql="$3" got
	if ! got="$("$DUCKDB" -unsigned -noheader -list -c "$PRELUDE$sql" 2>/tmp/loki_loadable_err)"; then
		echo "FAIL: $desc (query error)"; sed 's/^/      /' /tmp/loki_loadable_err; fails=$((fails + 1)); return
	fi
	if [[ "$got" == "$expected" ]]; then echo "PASS: $desc"; else
		echo "FAIL: $desc — expected [$expected], got [$got]"; fails=$((fails + 1)); fi
}

# assert_ok <description> <sql>  (query must succeed)
assert_ok() {
	local desc="$1" sql="$2"
	if "$DUCKDB" -unsigned -c "$PRELUDE$sql" >/dev/null 2>/tmp/loki_loadable_err; then
		echo "PASS: $desc"
	else
		echo "FAIL: $desc (query error)"; sed 's/^/      /' /tmp/loki_loadable_err; fails=$((fails + 1)); fi
}

# assert_error <description> <substring> <sql>  (query must fail and mention <substring>)
assert_error() {
	local desc="$1" needle="$2" sql="$3"
	if "$DUCKDB" -unsigned -c "$PRELUDE$sql" >/dev/null 2>/tmp/loki_loadable_err; then
		echo "FAIL: $desc — expected an error, query succeeded"; fails=$((fails + 1)); return
	fi
	if grep -q "$needle" /tmp/loki_loadable_err; then echo "PASS: $desc"; else
		echo "FAIL: $desc — error did not mention '$needle':"; sed 's/^/      /' /tmp/loki_loadable_err; fails=$((fails + 1)); fi
}

echo "==> Running loadable-binary smoke assertions"

# The loadable binary registers all five table functions.
assert "LOAD registers 5 loki table functions" 5 \
	"SELECT count(*) FROM duckdb_functions() WHERE function_name IN ('loki', 'loki_scan', 'loki_labels', 'loki_label_values', 'loki_series') AND function_type = 'table';"

# Bind resolves the fixed output schema offline (no network).
assert "loki_scan base schema is 4 columns" 4 \
	"SELECT count(*) FROM (DESCRIBE SELECT * FROM loki_scan('{job=\"x\"}', endpoint := 'http://localhost:3100'));"

# The loki secret type is registered by the loadable binary too (an unknown TYPE would error).
assert_ok "loki secret type registered" \
	"CREATE SECRET s (TYPE loki, ENDPOINT 'http://localhost:3100');"

# A representative error path fires before any network call (mandatory-selector rule, §4.3).
assert_error "loki() mandatory-selector error" "stream selector is required" \
	"SELECT * FROM loki(endpoint := 'http://localhost:3100');"

echo "==> Done"
if [[ "$fails" -gt 0 ]]; then echo "$fails assertion(s) failed." >&2; exit 1; fi
echo "All loadable-binary smoke assertions passed."
