"""Tests for the RAVEN Python SDK, running against a mock gateway served by
http.server in a background thread — no live stack needed."""

import json
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from raven import Client, RavenError, TokenPair


def job_json(job_id="job_1", status="QUEUED"):
    return {
        "id": job_id,
        "type": "webhook",
        "payload": {"url": "https://x.example"},
        "status": status,
        "priority": 5,
        "attempts": 0,
        "max_attempts": 4,
        "created_at": 1759998000,
        "started_at": 0,
        "finished_at": 0,
        "error": "",
        "worker_id": "",
        "scheduled_at": 0,
    }


class MockGateway(BaseHTTPRequestHandler):
    """Routes requests to per-test handler lambdas registered on the server."""

    protocol_version = "HTTP/1.1"

    def _dispatch(self):
        routes = self.server.routes
        key = (self.command, self.path.split("?")[0])
        handler = routes.get(key) or routes.get((self.command, "*"))
        if handler is None:
            body = json.dumps(
                {"error": {"code": "not_mocked", "message": f"no mock for {key}",
                           "request_id": "mock"}}
            ).encode()
            self.send_response(404)
        else:
            status, body = handler(self)
            if isinstance(body, (dict, list)):
                body = json.dumps(body).encode()
            self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = _dispatch
    do_POST = _dispatch
    do_DELETE = _dispatch
    do_PUT = _dispatch

    def read_json(self):
        length = int(self.headers.get("Content-Length") or 0)
        if not length:
            return None
        return json.loads(self.rfile.read(length))

    def log_message(self, *args):  # keep test output clean
        pass


class RavenTestCase(unittest.TestCase):
    """Base class wiring a throwaway mock gateway per test."""

    def setUp(self):
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), MockGateway)
        self.server.routes = {}
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.client = Client(base_url=f"http://127.0.0.1:{self.server.server_port}")

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def route(self, method, path):
        """Decorator registering a handler for (method, path)."""

        def deco(fn):
            self.server.routes[(method, path)] = fn
            return fn

        return deco


class TestAuth(RavenTestCase):
    def test_register(self):
        @self.route("POST", "/api/auth/register")
        def _(req):
            body = req.read_json()
            self.assertEqual(body["email"], "me@example.com")
            self.assertEqual(body["display_name"], "Me")
            return 201, {"user_id": "u-1", "email": "me@example.com"}

        resp = self.client.register("me@example.com", "12345678", display_name="Me")
        self.assertEqual(resp["user_id"], "u-1")

    def test_login_stores_tokens(self):
        @self.route("POST", "/api/auth/login")
        def _(req):
            return 200, {
                "access_token": "at-1",
                "refresh_token": "rt-1",
                "access_expires_at": 9999999999,
                "refresh_expires_at": 9999999999,
            }

        pair = self.client.login("me@example.com", "12345678")
        self.assertEqual(pair.access_token, "at-1")
        self.assertEqual(self.client.tokens.refresh_token, "rt-1")

    def test_bearer_header_sent(self):
        @self.route("GET", "/api/workers")
        def _(req):
            self.assertEqual(req.headers["Authorization"], "Bearer at-1")
            return 200, {"workers": []}

        self.client.tokens = TokenPair("at-1", "rt-1", 9999999999, 9999999999)
        self.client.list_workers()

    def test_logout_clears_tokens(self):
        seen = {}

        @self.route("POST", "/api/auth/logout")
        def _(req):
            seen.update(req.read_json())
            return 200, {"ok": True}

        self.client.tokens = TokenPair("at", "rt-gone", 9999999999, 9999999999)
        self.client.logout()
        self.assertEqual(seen["refresh_token"], "rt-gone")
        self.assertIsNone(self.client.tokens)

    def test_auto_refresh_on_expired_token(self):
        calls = {"refresh": 0, "bearer": None}

        @self.route("POST", "/api/auth/refresh")
        def _(req):
            calls["refresh"] += 1
            self.assertEqual(req.read_json()["refresh_token"], "rt-old")
            return 200, {
                "access_token": "at-new",
                "refresh_token": "rt-new",
                "access_expires_at": 9999999999,
                "refresh_expires_at": 9999999999,
            }

        @self.route("GET", "/api/jobs/job_1")
        def _(req):
            calls["bearer"] = req.headers["Authorization"]
            return 200, job_json(status="SUCCESS")

        self.client.tokens = TokenPair("at-old", "rt-old", int(time.time()) - 60, 9999999999)
        job = self.client.get_job("job_1")
        self.assertEqual(job.status, "SUCCESS")
        self.assertEqual(calls["refresh"], 1)
        self.assertEqual(calls["bearer"], "Bearer at-new")
        self.assertEqual(self.client.tokens.refresh_token, "rt-new")

    def test_retry_once_on_401(self):
        calls = {"jobs": 0}

        @self.route("POST", "/api/auth/refresh")
        def _(req):
            return 200, {
                "access_token": "at-new",
                "refresh_token": "rt-new",
                "access_expires_at": 9999999999,
                "refresh_expires_at": 9999999999,
            }

        @self.route("GET", "/api/jobs/job_1")
        def _(req):
            calls["jobs"] += 1
            if calls["jobs"] == 1:
                return 401, {"error": {"code": "token_expired", "message": "expired",
                                       "request_id": "r1"}}
            return 200, job_json(status="SUCCESS")

        # Access token looks fresh to the client, so no proactive refresh.
        self.client.tokens = TokenPair("at-old", "rt-old", 9999999999, 9999999999)
        job = self.client.get_job("job_1")
        self.assertEqual(job.status, "SUCCESS")
        self.assertEqual(calls["jobs"], 2)


class TestJobs(RavenTestCase):
    def test_create_job_sends_auto_idempotency_key(self):
        seen = {}

        @self.route("POST", "/api/jobs")
        def _(req):
            seen["key"] = req.headers.get("Idempotency-Key")
            seen["body"] = req.read_json()
            return 201, job_json()

        job = self.client.create_job("webhook", {"url": "https://x.example"}, priority=3)
        self.assertEqual(job.id, "job_1")
        self.assertFalse(job.terminal)
        self.assertTrue(seen["key"])  # auto-generated
        self.assertEqual(seen["body"]["priority"], 3)
        self.assertEqual(seen["body"]["payload"], {"url": "https://x.example"})

    def test_idempotency_key_override(self):
        seen = {}

        @self.route("POST", "/api/jobs")
        def _(req):
            seen["key"] = req.headers.get("Idempotency-Key")
            return 201, job_json()

        self.client.create_job(
            "webhook", {"url": "https://x.example"}, idempotency_key="retry-42"
        )
        self.assertEqual(seen["key"], "retry-42")

    def test_list_jobs_with_filters(self):
        @self.route("GET", "/api/jobs")
        def _(req):
            query = req.path.split("?", 1)[1]
            self.assertIn("status=failed", query)
            self.assertIn("page=2", query)
            return 200, {
                "jobs": [job_json("job_9", "FAILED")],
                "page": {"page": 2, "page_size": 20, "total": 1},
            }

        result = self.client.list_jobs(status="failed", page=2)
        self.assertEqual(len(result.jobs), 1)
        self.assertEqual(result.page.total, 1)
        self.assertTrue(result.jobs[0].terminal)

    def test_get_job(self):
        @self.route("GET", "/api/jobs/job_1")
        def _(req):
            return 200, job_json(status="PROCESSING")

        job = self.client.get_job("job_1")
        self.assertEqual(job.status, "PROCESSING")
        self.assertFalse(job.terminal)

    def test_cancel_requeue_replay(self):
        @self.route("POST", "/api/jobs/job_1/cancel")
        def _(req):
            return 200, job_json(status="CANCELLED")

        @self.route("POST", "/api/jobs/job_1/requeue")
        def _(req):
            return 200, job_json(status="QUEUED")

        @self.route("POST", "/api/jobs/job_1/replay")
        def _(req):
            clone = job_json("job_2", "QUEUED")
            clone["replayed_from"] = "job_1"
            return 201, clone

        self.assertEqual(self.client.cancel_job("job_1").status, "CANCELLED")
        self.assertEqual(self.client.requeue_job("job_1").status, "QUEUED")
        replay = self.client.replay_job("job_1")
        self.assertEqual(replay.replayed_from, "job_1")
        self.assertEqual(replay.id, "job_2")

    def test_deliveries_nullable_fields(self):
        @self.route("GET", "/api/jobs/job_1/deliveries")
        def _(req):
            return 200, {
                "deliveries": [
                    {"id": 41, "job_id": "job_1", "attempt": 1,
                     "url": "https://x.example", "status_code": 200,
                     "latency_ms": 83, "response_snippet": "{}",
                     "blocked": False, "error": "", "ts": 1767225600},
                    {"id": 42, "job_id": "job_1", "attempt": 2,
                     "url": "https://x.example", "status_code": None,
                     "latency_ms": None, "response_snippet": "",
                     "blocked": True, "error": "egress guard", "ts": 1767225601},
                ],
                "page": {"page": 1, "page_size": 20, "total": 2},
            }

        result = self.client.job_deliveries("job_1")
        self.assertEqual(len(result.deliveries), 2)
        self.assertEqual(result.deliveries[0].status_code, 200)
        self.assertIsNone(result.deliveries[1].status_code)
        self.assertTrue(result.deliveries[1].blocked)

    def test_watch_job_until_terminal(self):
        calls = {"n": 0}

        @self.route("GET", "/api/jobs/job_1")
        def _(req):
            calls["n"] += 1
            status = "PROCESSING" if calls["n"] < 3 else "SUCCESS"
            return 200, job_json(status=status)

        final = self.client.watch_job("job_1", interval=0.01, timeout=5)
        self.assertEqual(final.status, "SUCCESS")
        self.assertGreaterEqual(calls["n"], 3)

    def test_watch_job_timeout(self):
        @self.route("GET", "/api/jobs/job_1")
        def _(req):
            return 200, job_json(status="PROCESSING")

        with self.assertRaises(RavenError) as ctx:
            self.client.watch_job("job_1", interval=0.01, timeout=0.1)
        self.assertEqual(ctx.exception.code, "watch_timeout")

    def test_iter_job_yields_transitions(self):
        calls = {"n": 0}

        @self.route("GET", "/api/jobs/job_1")
        def _(req):
            calls["n"] += 1
            status = ["QUEUED", "PROCESSING", "SUCCESS"][min(calls["n"] - 1, 2)]
            return 200, job_json(status=status)

        seen = [j.status for j in self.client.iter_job("job_1", interval=0.01)]
        self.assertEqual(seen[-1], "SUCCESS")
        self.assertIn("QUEUED", seen)


class TestCronsKeysOps(RavenTestCase):
    def test_cron_lifecycle(self):
        @self.route("POST", "/api/crons")
        def _(req):
            body = req.read_json()
            self.assertEqual(body["cron_expr"], "0 3 * * *")
            return 201, {
                "id": "cron_1", "name": "nightly", "cron_expr": "0 3 * * *",
                "type": "webhook", "payload": {"url": "https://x.example"},
                "priority": 5, "enabled": True, "next_run_at": 1767225600,
                "last_run_at": 0, "created_at": 1767220000,
            }

        @self.route("GET", "/api/crons")
        def _(req):
            return 200, {"crons": [{"id": "cron_1", "name": "n", "cron_expr": "0 3 * * *",
                                    "type": "webhook", "payload": {}, "priority": 5,
                                    "enabled": True, "next_run_at": 1, "last_run_at": 0,
                                    "created_at": 1}],
                         "page": {"page": 1, "page_size": 20, "total": 1}}

        @self.route("DELETE", "/api/crons/cron_1")
        def _(req):
            return 200, {"ok": True}

        cron = self.client.create_cron(
            "nightly", "0 3 * * *", "webhook", {"url": "https://x.example"}
        )
        self.assertEqual(cron.next_run_at, 1767225600)
        self.assertTrue(cron.enabled)
        self.assertEqual(self.client.list_crons().page.total, 1)
        self.client.delete_cron("cron_1")  # no exception

    def test_api_key_lifecycle(self):
        @self.route("POST", "/api/keys")
        def _(req):
            body = req.read_json()
            self.assertEqual(body["scopes"], ["jobs:read"])
            return 201, {
                "key": "rav_live_secret",
                "api_key": {"id": "k1", "name": "ci", "prefix": "rav_live_9f2k",
                            "scopes": ["jobs:read"],
                            "created_at": "2026-01-01T12:00:00Z", "last_used_at": None},
            }

        @self.route("GET", "/api/keys")
        def _(req):
            return 200, {"api_keys": [{"id": "k1", "name": "ci", "prefix": "rav_live_9f2k",
                                       "scopes": ["jobs:read"],
                                       "created_at": "2026-01-01T12:00:00Z",
                                       "last_used_at": "2026-01-02T08:30:00Z"}]}

        @self.route("DELETE", "/api/keys/k1")
        def _(req):
            return 200, {"ok": True}

        result = self.client.create_api_key("ci", ["jobs:read"])
        self.assertEqual(result.key, "rav_live_secret")
        self.assertEqual(result.api_key.prefix, "rav_live_9f2k")

        keys = self.client.list_api_keys()
        self.assertEqual(keys[0].last_used_at, "2026-01-02T08:30:00Z")

        self.client.revoke_api_key("k1")

    def test_api_key_auth_header(self):
        @self.route("GET", "/api/workers")
        def _(req):
            self.assertEqual(req.headers["Authorization"], "ApiKey rav_live_abc")
            return 200, {"workers": [{"id": "w-1", "started_at": "2025-10-09T12:00:00Z",
                                      "last_heartbeat": "2025-10-09T12:04:35Z",
                                      "jobs_processed": "138", "in_flight": "2"}]}

        client = Client(
            base_url=self.client.base_url, api_key="rav_live_abc"
        )
        workers = client.list_workers()
        self.assertEqual(workers[0].jobs_processed, "138")

    def test_health_services(self):
        @self.route("GET", "/api/health/services")
        def _(req):
            return 200, {
                "checked_at": "2026-01-01T12:00:00Z",
                "services": [
                    {"name": "gateway", "status": "ok", "latency_ms": 0, "detail": "self"},
                    {"name": "worker_pool", "status": "degraded", "latency_ms": 5,
                     "detail": "no workers registered"},
                ],
            }

        report = self.client.health_services()
        self.assertEqual(len(report.services), 2)
        self.assertEqual(report.services[1].status, "degraded")

    def test_audit_list(self):
        @self.route("GET", "/api/audit")
        def _(req):
            query = req.path.split("?", 1)[1]
            self.assertIn("action=job.cancel", query)
            self.assertIn("limit=10", query)
            return 200, {
                "events": [{"id": 7, "ts": "2026-01-01T12:00:00Z", "actor_id": "u-1",
                            "action": "job.cancel", "resource_type": "job",
                            "resource_id": "job_1", "outcome": "allowed"}],
                "next_before_id": 7,
            }

        result = self.client.list_audit_events(action="job.cancel", limit=10)
        self.assertEqual(result.events[0].outcome, "allowed")
        self.assertEqual(result.next_before_id, 7)


class TestErrors(RavenTestCase):
    def test_error_envelope(self):
        @self.route("GET", "/api/jobs/job_nope")
        def _(req):
            return 404, {"error": {"code": "job_not_found",
                                   "message": "job does not exist",
                                   "request_id": "req-123"}}

        with self.assertRaises(RavenError) as ctx:
            self.client.get_job("job_nope")
        err = ctx.exception
        self.assertEqual(err.code, "job_not_found")
        self.assertEqual(err.message, "job does not exist")
        self.assertEqual(err.request_id, "req-123")
        self.assertEqual(err.status_code, 404)
        self.assertIn("req-123", str(err))

    def test_non_envelope_error_body(self):
        @self.route("GET", "/api/jobs/job_1")
        def _(req):
            return 502, b"<html>proxy exploded</html>"

        with self.assertRaises(RavenError) as ctx:
            self.client.get_job("job_1")
        self.assertEqual(ctx.exception.code, "http_502")
        self.assertEqual(ctx.exception.status_code, 502)


class TestTransport(RavenTestCase):
    def test_transport_error(self):
        # Nothing listens on this port.
        client = Client(base_url="http://127.0.0.1:1", timeout=1)
        with self.assertRaises(RavenError) as ctx:
            client.list_workers()
        self.assertEqual(ctx.exception.code, "transport_error")


if __name__ == "__main__":
    unittest.main()
