"""Run with `make build` and `python3 tests/e2e/smoke.py bin/chaos-proxy`."""

import argparse
from collections import Counter
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import time


LOOPBACK = "127.0.0.1"
TOKEN = "e2e-chaos-token"
PRIVATE_HEADER = "e2e-application-header-value"
PRIVATE_QUERY = "e2e-private-query-value"


class Backend(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({"path": self.path, "headers": dict(self.headers)}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format, *_args):
        pass


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def get(address, path, headers=None, expected_status=200):
    connection = HTTPConnection(address, timeout=5)
    try:
        connection.request("GET", path, headers=headers or {})
        with connection.getresponse() as response:
            check(
                response.status == expected_status,
                f"GET {address}{path}: got {response.status}, expected {expected_status}",
            )
            return response.read()
    finally:
        connection.close()


def wait_for_logs(process, path, expected):
    deadline = time.monotonic() + 10
    while True:
        output = path.read_text(encoding="utf-8")
        complete_lines = output.split("\n")[:-1]
        try:
            records = [json.loads(line) for line in complete_lines]
        except json.JSONDecodeError as error:
            raise AssertionError(f"expected JSON logs; got {output!r}") from error
        counts = Counter(record["msg"] for record in records)
        if all(counts[message] >= count for message, count in expected.items()):
            return records
        if process.poll() is not None or time.monotonic() >= deadline:
            raise AssertionError(f"expected {expected} in process logs; got {output!r}")
        time.sleep(0.02)


def exercise(process, log_path, upstream_address):
    listeners = {"proxy listening": 1, "admin listening": 1}
    startup = wait_for_logs(process, log_path, listeners)
    addresses = {record["msg"]: record["address"] for record in startup if record["msg"] in listeners}
    proxy = addresses["proxy listening"]
    admin = addresses["admin listening"]
    check(proxy != admin, "the proxy and administrative listeners shared an address")

    for probe in ("/healthz", "/readyz"):
        check(get(admin, probe) == b"ok\n", f"unexpected administrative response for {probe}")
    version = json.loads(get(admin, "/version")).get("version")
    check(isinstance(version, str) and version, "the build version was missing")
    get(admin, "/controlled", expected_status=404)

    ordinary = json.loads(get(proxy, "/healthz"))
    check(ordinary["path"] == "/healthz", "the proxy listener did not route ordinary traffic")
    get(proxy, "/unauthorized", {"X-Chaos-Response-Delay-Ms": "1"}, expected_status=403)
    controlled_path = f"/controlled?q={PRIVATE_QUERY}"
    controlled = get(
        proxy,
        controlled_path,
        {
            "X-Chaos-Enabled": "true",
            "X-Chaos-Response-Delay-Ms": "1",
            "X-Chaos-Token": TOKEN,
            "X-Smoke-Private": PRIVATE_HEADER,
        },
    )
    forwarded = json.loads(controlled)
    headers = {name.lower(): value for name, value in forwarded["headers"].items()}
    check(forwarded["path"] == controlled_path, "the backend did not receive the query")
    check(headers.get("x-smoke-private") == PRIVATE_HEADER, "an application header was dropped")
    check(not any(name.startswith("x-chaos-") for name in headers), "the backend saw a control header")
    check(TOKEN.encode() not in controlled, "the backend saw the chaos token")

    records = wait_for_logs(process, log_path, {"proxy request": 3})
    requests = {record["path"]: record for record in records if record["msg"] == "proxy request"}
    expected_statuses = {"/healthz": 200, "/unauthorized": 403, "/controlled": 200}
    check(set(requests) == set(expected_statuses), f"unexpected request log paths: {list(requests)}")
    request_ids = set()
    for path, record in requests.items():
        applied = path == "/controlled"
        expected = {
            "method": "GET",
            "upstream": upstream_address,
            "status_code": expected_statuses[path],
            "request_delay_ms": 0,
            "response_delay_ms": int(applied),
        }
        for key, value in expected.items():
            check(record.get(key) == value, f"{path}: expected log field {key}={value!r}")
        check(record.get("chaos_applied") is applied, f"{path}: unexpected chaos_applied")
        check(record.get("canceled") is False, f"{path}: unexpected canceled")
        request_id = record.get("request_id")
        check(isinstance(request_id, str) and request_id, f"{path}: missing request_id")
        request_ids.add(request_id)
        for key in ("upstream_duration_ms", "total_duration_ms"):
            value = record.get(key)
            check(isinstance(value, (int, float)) and value >= 0, f"{path}: invalid {key}")
    check(len(request_ids) == len(requests), "the request logs reused a request ID")

    metrics = set(get(admin, "/metrics").decode().splitlines())
    for sample in (
        'chaos_proxy_requests_total{method="GET",status_code="200"} 2',
        'chaos_proxy_requests_total{method="GET",status_code="403"} 1',
        "chaos_proxy_request_duration_seconds_count 3",
        'chaos_proxy_injections_total{direction="response",fault_type="delay"} 1',
    ):
        check(sample in metrics, f"the metrics scrape was missing {sample!r}")


def smoke(binary):
    environment = {
        name: value for name, value in os.environ.items()
        if not name.startswith(("SERVER_", "UPSTREAM_", "OBSERVABILITY_", "CHAOS_"))
    }
    with tempfile.TemporaryDirectory() as directory:
        log_path = Path(directory) / "stderr.log"
        with ThreadingHTTPServer((LOOPBACK, 0), Backend) as backend:
            thread = threading.Thread(target=backend.serve_forever, daemon=True)
            thread.start()
            upstream_address = f"{LOOPBACK}:{backend.server_port}"
            environment.update(
                SERVER_LISTEN="overridden-by-listen-flag",
                UPSTREAM_URL=f"http://{upstream_address}",
                OBSERVABILITY_LISTEN=f"{LOOPBACK}:0",
                CHAOS_TOKEN=TOKEN,
            )
            try:
                with log_path.open("wb") as stderr, subprocess.Popen(
                    [binary, "-listen", f"{LOOPBACK}:0"], env=environment, stderr=stderr,
                ) as process:
                    try:
                        exercise(process, log_path, upstream_address)
                    finally:
                        process.terminate()
                        try:
                            process.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait()
                if os.name == "posix":
                    check(process.returncode == 0, f"proxy exited with status {process.returncode}")
                output = log_path.read_text(encoding="utf-8").lower()
                for secret in (TOKEN, PRIVATE_HEADER, PRIVATE_QUERY, "x-chaos-", "x-smoke-private"):
                    check(secret not in output, "the process logs disclosed request data")
            finally:
                backend.shutdown()
                thread.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path, help="path to the built chaos-proxy executable")
    smoke(parser.parse_args().binary.resolve())
    print("chaos-proxy end-to-end smoke test passed")
