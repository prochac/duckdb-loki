#!/usr/bin/env python3
"""A tiny, deterministic mock of the Loki HTTP API for sqllogictest (DESIGN.md §8).

No real Loki, no network flakiness: it answers the read endpoints the extension calls
(`/query_range`, `/labels`, `/label/{name}/values`, `/series`) with canned payloads, so
end-to-end SQL can exercise the streams parser, projection / label promotion, error
surfacing, the discovery helpers, and — crucially — *filter pushdown translation*.

Two response modes for `/query_range`, selected by a marker token in the LogQL `query`:

  * echo mode  (query contains `__echo__`): return a single entry whose `line` is the
    verbatim LogQL the extension built, and whose structured metadata carries the request
    params (`req_query`, `req_start`, `req_end`, `req_limit`, `req_direction`) and the
    resolved auth (`req_auth`, `req_tenant`). This turns "what did the extension ask Loki?"
    into queryable rows, so `loki()`'s WHERE→LogQL translation can be asserted directly.
    The single entry carries benign stream labels ({job,level,pod}) and a timestamp equal
    to the received `start`, so predicates the extension pushes-but-keeps-residual
    (`!=`, timestamp bounds) still match it and the row survives DuckDB's re-filtering.

  * fixture mode (any other marker): canned streams for the parser/shape tests
      `__stream__` — two streams / three entries, rich labels + structured metadata
      `__empty__`  — a valid streams response with no results
      `__error__`  — HTTP 400 with an error body, to test error surfacing

Deliberately NOT modelled here: paging. The pager's per-request cap matches Loki's server
cap (5000), so triggering real paging needs 5000+ entries — impractical and already covered
end-to-end by the Docker integration test (test/integration/). This mock returns every
matching entry in a single page.

Usage: mock_loki.py [port]   (0 or omitted → an ephemeral port; the chosen port is printed).
Stdlib only, so CI needs no pip install.
"""

import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

API = "/loki/api/v1"


def streams_body(result):
    return {"status": "success", "data": {"resultType": "streams", "result": result}}


# The canonical rich fixture for `__stream__`: two streams, three entries. Timestamps use all
# 19 digits of nanosecond precision so the test can assert the extension preserves raw ns
# (not silently rescaled to microseconds). Structured metadata is present, empty, and
# multi-key across the three entries.
STREAM_FIXTURE = streams_body(
    [
        {
            "stream": {"job": "api", "level": "error", "pod": "api-1"},
            "values": [
                ["1700000000000000001", "boom one", {"structuredMetadata": {"trace_id": "t-aaa"}, "parsed": {}}],
                ["1700000000000000002", "boom two", {"structuredMetadata": {}, "parsed": {}}],
            ],
        },
        {
            "stream": {"job": "api", "level": "info", "pod": "api-2"},
            "values": [
                [
                    "1700000000000000003",
                    "hello world",
                    {"structuredMetadata": {"trace_id": "t-bbb", "span_id": "s-1"}, "parsed": {}},
                ],
            ],
        },
    ]
)

# Discovery fixtures.
LABELS_FIXTURE = {"status": "success", "data": ["job", "level", "pod"]}
LABEL_VALUES_FIXTURE = {"status": "success", "data": ["api", "web"]}
SERIES_FIXTURE = {
    "status": "success",
    "data": [
        {"job": "api", "level": "error"},
        {"job": "api", "level": "info"},
    ],
}


def echo_body(params, auth, tenant):
    query = params.get("query", [""])[0]
    start = params.get("start", [""])[0]
    end = params.get("end", [""])[0]
    limit = params.get("limit", [""])[0]
    direction = params.get("direction", [""])[0]
    # Timestamp = the received `start` so residual timestamp / `!=` filters keep the row.
    ts = start if start else "1700000000000000000"
    meta = {
        "req_query": query,
        "req_start": start,
        "req_end": end,
        "req_limit": limit,
        "req_direction": direction,
        "req_auth": auth,
        "req_tenant": tenant,
    }
    return streams_body(
        [
            {
                "stream": {"job": "echo", "level": "info", "pod": "p1"},
                "values": [[ts, query, {"structuredMetadata": meta, "parsed": {}}]],
            },
        ]
    )


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass  # quiet: keep the test output clean

    def _send_json(self, status, obj):
        payload = json.dumps(obj).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _auth(self):
        header = self.headers.get("Authorization", "")
        if header.startswith("Bearer "):
            return "bearer"
        if header.startswith("Basic "):
            return "basic"
        return ""

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query)

        if path == "/ready":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ready")
            return

        if path == API + "/query_range":
            query = params.get("query", [""])[0]
            if "__echo__" in query:
                self._send_json(200, echo_body(params, self._auth(), self.headers.get("X-Scope-OrgID", "")))
            elif "__error__" in query:
                self._send_json(400, {"status": "error", "error": "mock injected error: parse error at line 1"})
            elif "__empty__" in query:
                self._send_json(200, streams_body([]))
            else:
                self._send_json(200, STREAM_FIXTURE)
            return

        if path == API + "/labels":
            self._send_json(200, LABELS_FIXTURE)
            return

        # /label/{name}/values
        if path.startswith(API + "/label/") and path.endswith("/values"):
            self._send_json(200, LABEL_VALUES_FIXTURE)
            return

        if path == API + "/series":
            self._send_json(200, SERIES_FIXTURE)
            return

        self._send_json(404, {"status": "error", "error": "mock: unknown path " + path})


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    # Print the bound port so the caller can discover an ephemeral one, then serve forever.
    print(server.server_address[1], flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
