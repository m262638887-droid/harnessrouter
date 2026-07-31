#!/usr/bin/env python3
"""Cline free-model proxy.

Injects Cline product-surface headers so cline-free/* works.
Auth key is passed through from upstream channel (Bearer / multi-key polling).
"""

import http.server
import socketserver
import json
import os
import sys
import threading
from urllib.parse import urlparse

LISTEN_HOST = os.environ.get("CLINE_PROXY_HOST", "0.0.0.0")
LISTEN_PORT = int(os.environ.get("CLINE_PROXY_PORT", "3015"))
# Upstream goes through worker (sandbox egress)
UPSTREAM_BASE = os.environ.get(
    "CLINE_UPSTREAM",
    "https://api.cline.bot/api/v1",
)

# Official headers from Cline source (request-headers.ts). Key one is X-CLIENT-TYPE.
CLINE_HEADERS = {
    "HTTP-Referer": "https://cline.bot",
    "X-Title": "Cline",
    "X-IS-MULTIROOT": "false",
    "X-CLIENT-TYPE": os.environ.get("CLINE_CLIENT_TYPE", "cline-cli"),
    "X-CLIENT-VERSION": os.environ.get("CLINE_CLIENT_VERSION", "3.0.38"),
    "X-PLATFORM": os.environ.get("CLINE_PLATFORM", "cli"),
    "X-PLATFORM-VERSION": os.environ.get("CLINE_PLATFORM_VERSION", "3.0.38"),
    "X-CORE-VERSION": os.environ.get("CLINE_CORE_VERSION", "0.2.0"),
    "X-Task-ID": os.environ.get("CLINE_TASK_ID", "new-api"),
}

HOP_BY_HOP = {
    "host",
    "content-length",
    "transfer-encoding",
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "upgrade",
    "accept-encoding",
}


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        sys.stderr.write("[cline-proxy] %s - %s\n" % (self.address_string(), fmt % args))
        sys.stderr.flush()

    def do_GET(self):
        self._proxy("GET")

    def do_POST(self):
        self._proxy("POST")

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.end_headers()

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0) or 0)
        if length <= 0:
            return None
        return self.rfile.read(length)

    def _build_headers(self):
        headers = {}
        # pass through client headers (esp. Authorization)
        for k, v in self.headers.items():
            if k.lower() in HOP_BY_HOP:
                continue
            headers[k] = v
        # inject / override cline product surface headers
        for k, v in CLINE_HEADERS.items():
            headers[k] = v
        headers["Accept-Encoding"] = "identity"
        if "Content-Type" not in {x.title(): 1 for x in headers} and "content-type" not in {x.lower(): 1 for x in headers}:
            # keep existing content-type if any
            pass
        # normalize: ensure content-type for JSON posts if missing handled upstream
        return headers

    def _proxy(self, method):
        import requests
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        path = self.path or "/"
        # new-api may call /v1/chat/completions or /chat/completions
        # upstream base already ends with /api/v1
        if path.startswith("/v1/"):
            path = path[3:]  # drop leading /v1 -> /chat/completions
        target = UPSTREAM_BASE.rstrip("/") + path

        body = self._read_body() if method in ("POST", "PUT", "PATCH") else None
        headers = self._build_headers()

        try:
            resp = requests.request(
                method,
                target,
                data=body,
                headers=headers,
                timeout=300,
                verify=False,
                stream=True,
            )
        except Exception as e:
            payload = json.dumps({"error": {"message": "cline-proxy upstream error: %s" % e}}).encode()
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        # streaming vs non-streaming
        ctype = (resp.headers.get("Content-Type") or "").lower()
        is_stream = "text/event-stream" in ctype or "stream" in ctype

        self.send_response(resp.status_code)
        skip = {"transfer-encoding", "connection", "content-encoding", "content-length"}
        for k, v in resp.headers.items():
            if k.lower() in skip:
                continue
            self.send_header(k, v)

        if is_stream:
            self.send_header("Cache-Control", "no-cache")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()
            try:
                for chunk in resp.iter_content(chunk_size=4096):
                    if chunk:
                        self.wfile.write(chunk)
                        self.wfile.flush()
            except Exception as e:
                sys.stderr.write("[cline-proxy] stream err: %s\n" % e)
                sys.stderr.flush()
            finally:
                resp.close()
            return

        data = resp.content
        # Cline wraps as {"data": {...}, "success": true} sometimes — unwrap for OpenAI clients
        try:
            if data and "application/json" in ctype:
                obj = json.loads(data)
                if isinstance(obj, dict) and obj.get("success") is True and isinstance(obj.get("data"), dict):
                    # if data looks like chat completion, unwrap
                    inner = obj["data"]
                    if "choices" in inner or "object" in inner or "id" in inner:
                        data = json.dumps(inner, ensure_ascii=False).encode("utf-8")
                        # strip content-type charset issues
        except Exception:
            pass

        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
        resp.close()


class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    srv = ThreadedHTTPServer((LISTEN_HOST, LISTEN_PORT), Handler)
    print(
        "cline-proxy on %s:%s -> %s" % (LISTEN_HOST, LISTEN_PORT, UPSTREAM_BASE),
        flush=True,
    )
    print("inject headers: %s" % json.dumps(CLINE_HEADERS, ensure_ascii=False), flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
