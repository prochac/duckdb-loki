#!/usr/bin/env bash
#
# Push a small, fixed set of log lines into Loki via POST /loki/api/v1/push and print the base
# epoch (seconds) the entries are anchored to on stdout — the test reads it to build exact time
# windows. Everything else goes to stderr. Timestamps are recent (base = now - 300s) so they stay
# inside Loki's ingester window and out of any "reject old sample" bound.
#
# Seeded data (base + offset seconds):
#   +0s  {job=api,level=info}   GET /health 200
#   +1s  {job=api,level=info}   GET /users 200
#   +2s  {job=api,level=error}  connection timeout to upstream
#   +3s  {job=api,level=error}  database error: deadlock
#   +4s  {job=web,level=info}   render homepage
#   +5s  {job=web,level=warn}   slow response timeout warning
#
set -euo pipefail

LOKI_URL="${1:-http://localhost:3100}"
BASE=$(( $(date -u +%s) - 300 ))

ns() { echo "$(( (BASE + $1) * 1000000000 ))"; }

# Values within each stream must be in ascending timestamp order (Loki requirement).
payload=$(jq -n \
	--arg t0 "$(ns 0)" --arg t1 "$(ns 1)" --arg t2 "$(ns 2)" \
	--arg t3 "$(ns 3)" --arg t4 "$(ns 4)" --arg t5 "$(ns 5)" \
	'{streams: [
		{stream: {job: "api", level: "info"},  values: [[$t0, "GET /health 200"], [$t1, "GET /users 200"]]},
		{stream: {job: "api", level: "error"}, values: [[$t2, "connection timeout to upstream"], [$t3, "database error: deadlock"]]},
		{stream: {job: "web", level: "info"},  values: [[$t4, "render homepage"]]},
		{stream: {job: "web", level: "warn"},  values: [[$t5, "slow response timeout warning"]]}
	]}')

echo "Pushing 6 entries to $LOKI_URL (base epoch $BASE)" >&2
curl -sf -H 'Content-Type: application/json' -X POST "$LOKI_URL/loki/api/v1/push" -d "$payload" >&2

echo "$BASE"
