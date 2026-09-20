"""Core client for the RAVEN Python SDK. Standard library only."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import Any, Callable, Dict, Iterator, List, Optional

from .errors import RavenError
from .models import (
    APIKey,
    AuditEvent,
    AuditList,
    CreateAPIKeyResult,
    Cron,
    CronList,
    Delivery,
    DeliveryList,
    HealthReport,
    Job,
    JobList,
    Page,
    ServiceStatus,
    TokenPair,
    Worker,
)

DEFAULT_BASE_URL = "http://localhost:8080"
DEFAULT_TIMEOUT = 10.0  # seconds

# Refresh a little before the stated expiry so a request never flies with a
# token that dies mid-flight.
_REFRESH_SKEW = 30.0

USER_AGENT = "raven-python-sdk/1.0"


def _new_idempotency_key() -> str:
    """Random 128-bit hex key, well under the API's 255-char cap."""
    return uuid.uuid4().hex


class Client:
    """Talks to the RAVEN gateway's public REST API.

    Args:
        base_url: gateway address; a trailing slash is trimmed.
        api_key: static machine credential (``rav_live_...``). When set, the
            client sends ``Authorization: ApiKey <key>`` and never refreshes.
        tokens: an existing :class:`TokenPair`, e.g. one persisted from an
            earlier session. It is refreshed automatically when the access
            token nears expiry.
        timeout: per-request timeout in seconds (default 10).
        idempotency_key_factory: override how ``Idempotency-Key`` values are
            generated for mutating requests (mostly for tests).

    The client is not thread-safe around token rotation; if you share one
    across threads, guard calls with your own lock or use one client per
    thread.
    """

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        *,
        api_key: Optional[str] = None,
        tokens: Optional[TokenPair] = None,
        timeout: float = DEFAULT_TIMEOUT,
        idempotency_key_factory: Optional[Callable[[], str]] = None,
        opener: Optional[urllib.request.OpenerDirector] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.tokens = tokens
        self.timeout = timeout
        self._idem_key_fn = idempotency_key_factory or _new_idempotency_key
        # An injectable opener keeps tests free of monkey-patching globals.
        self._opener = opener or urllib.request.build_opener()

    # ------------------------------------------------------------------
    # Request plumbing
    # ------------------------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        body: Optional[Dict[str, Any]] = None,
        *,
        mutating: bool = False,
        idempotency_key: Optional[str] = None,
        query: Optional[Dict[str, Any]] = None,
        _retried: bool = False,
    ) -> Any:
        self._ensure_fresh_token()

        url = self.base_url + path
        if query:
            qs = urllib.parse.urlencode({k: v for k, v in query.items() if v is not None})
            if qs:
                url += "?" + qs

        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Accept", "application/json")
        req.add_header("User-Agent", USER_AGENT)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        if mutating:
            req.add_header("Idempotency-Key", idempotency_key or self._idem_key_fn())
        if self.api_key:
            req.add_header("Authorization", f"ApiKey {self.api_key}")
        elif self.tokens and self.tokens.access_token:
            req.add_header("Authorization", f"Bearer {self.tokens.access_token}")

        try:
            with self._opener.open(req, timeout=self.timeout) as resp:
                raw = resp.read()
                status = resp.status
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            status = exc.code
            # One silent retry: the access token died between the freshness
            # check and the server validation (clock skew).
            if (
                status == 401
                and not _retried
                and not self.api_key
                and self.tokens
                and self.tokens.refresh_token
            ):
                self._refresh_now()
                return self._request(
                    method,
                    path,
                    body,
                    mutating=mutating,
                    idempotency_key=idempotency_key,
                    query=query,
                    _retried=True,
                )
            raise self._decode_error(status, raw) from None
        except urllib.error.URLError as exc:
            raise RavenError(
                code="transport_error",
                message=f"{method} {path} failed: {exc.reason}",
            ) from exc

        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RavenError(
                code="bad_response",
                message=f"{method} {path} returned invalid JSON",
                status_code=status,
            ) from exc

    @staticmethod
    def _decode_error(status: int, raw: bytes) -> RavenError:
        try:
            envelope = json.loads(raw)
            err = envelope.get("error") or {}
            if err.get("code"):
                return RavenError(
                    code=err["code"],
                    message=err.get("message", ""),
                    request_id=err.get("request_id"),
                    status_code=status,
                )
        except json.JSONDecodeError:
            pass
        return RavenError(
            code=f"http_{status}",
            message=f"HTTP {status}",
            status_code=status,
        )

    # ------------------------------------------------------------------
    # Token lifecycle
    # ------------------------------------------------------------------

    def _ensure_fresh_token(self) -> None:
        """Refresh the pair when the access token is expired or close to it.

        No-op for API-key auth.
        """
        if self.api_key or not self.tokens or not self.tokens.refresh_token:
            return
        if time.time() + _REFRESH_SKEW >= self.tokens.access_expires_at:
            self._refresh_now()

    def _refresh_now(self) -> None:
        if not self.tokens or not self.tokens.refresh_token:
            raise RavenError(code="no_refresh_token", message="client has no refresh token")
        req = urllib.request.Request(
            self.base_url + "/api/auth/refresh",
            data=json.dumps({"refresh_token": self.tokens.refresh_token}).encode("utf-8"),
            method="POST",
        )
        req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "application/json")
        req.add_header("User-Agent", USER_AGENT)
        try:
            with self._opener.open(req, timeout=self.timeout) as resp:
                data = json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            raise self._decode_error(exc.code, exc.read()) from None
        except urllib.error.URLError as exc:
            raise RavenError(
                code="transport_error", message=f"token refresh failed: {exc.reason}"
            ) from exc
        self.tokens = TokenPair.from_dict(data)

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def register(self, email: str, password: str, display_name: str = "") -> Dict[str, str]:
        """Create a USER-role account. Does not log the user in — call
        :meth:`login` next."""
        body: Dict[str, Any] = {"email": email, "password": password}
        if display_name:
            body["display_name"] = display_name
        return self._request("POST", "/api/auth/register", body)

    def login(self, email: str, password: str) -> TokenPair:
        """Exchange credentials for a token pair and store it on the client.
        Every later call is authenticated and auto-refreshed."""
        data = self._request("POST", "/api/auth/login", {"email": email, "password": password})
        self.tokens = TokenPair.from_dict(data)
        return self.tokens

    def refresh(self) -> TokenPair:
        """Rotate the stored refresh token for a fresh pair. Normally
        automatic; exposed for explicit session management."""
        self._refresh_now()
        assert self.tokens is not None
        return self.tokens

    def logout(self) -> None:
        """Kill the session behind the stored refresh token and clear the
        local pair. Idempotent server-side."""
        if self.tokens and self.tokens.refresh_token:
            self._request(
                "POST", "/api/auth/logout", {"refresh_token": self.tokens.refresh_token}
            )
        self.tokens = None

    # ------------------------------------------------------------------
    # Jobs
    # ------------------------------------------------------------------

    def create_job(
        self,
        type: str,
        payload: Dict[str, Any],
        *,
        priority: Optional[int] = None,
        max_attempts: Optional[int] = None,
        scheduled_at: Optional[int] = None,
        idempotency_key: Optional[str] = None,
    ) -> Job:
        """Queue a job. An ``Idempotency-Key`` is generated automatically;
        pass ``idempotency_key`` to pin it when retrying after a timeout —
        the same key always returns the same job.

        ``scheduled_at`` is an optional Unix-seconds time in the future;
        omit it to run immediately.
        """
        body: Dict[str, Any] = {"type": type, "payload": payload}
        if priority is not None:
            body["priority"] = priority
        if max_attempts is not None:
            body["max_attempts"] = max_attempts
        if scheduled_at is not None:
            body["scheduled_at"] = scheduled_at
        data = self._request(
            "POST", "/api/jobs", body, mutating=True, idempotency_key=idempotency_key
        )
        return Job.from_dict(data)

    def list_jobs(
        self,
        *,
        status: Optional[str] = None,
        type: Optional[str] = None,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
    ) -> JobList:
        """Page through jobs. ``status`` accepts "QUEUED", "queued" or
        "JOB_STATUS_QUEUED"; ``type`` is an exact match."""
        data = self._request(
            "GET",
            "/api/jobs",
            query={"status": status, "type": type, "page": page, "page_size": page_size},
        )
        return JobList(
            jobs=[Job.from_dict(j) for j in data.get("jobs", [])],
            page=Page.from_dict(data.get("page", {})),
        )

    def get_job(self, job_id: str) -> Job:
        """Fetch one job by id."""
        data = self._request("GET", f"/api/jobs/{urllib.parse.quote(job_id, safe='')}")
        return Job.from_dict(data)

    def cancel_job(self, job_id: str, *, idempotency_key: Optional[str] = None) -> Job:
        """Move a QUEUED/RETRYING/SCHEDULED job to CANCELLED. Jobs that
        already moved on answer ``409 job_not_cancellable``."""
        data = self._request(
            "POST",
            f"/api/jobs/{urllib.parse.quote(job_id, safe='')}/cancel",
            mutating=True,
            idempotency_key=idempotency_key,
        )
        return Job.from_dict(data)

    def requeue_job(self, job_id: str, *, idempotency_key: Optional[str] = None) -> Job:
        """Resurrect a DEAD job to QUEUED and republish it. Any other status
        answers ``409 job_not_dead``."""
        data = self._request(
            "POST",
            f"/api/jobs/{urllib.parse.quote(job_id, safe='')}/requeue",
            mutating=True,
            idempotency_key=idempotency_key,
        )
        return Job.from_dict(data)

    def replay_job(self, job_id: str, *, idempotency_key: Optional[str] = None) -> Job:
        """Clone a job into a brand-new one (fresh id, zero attempts,
        ``replayed_from`` pointing at the source). Works on any state."""
        data = self._request(
            "POST",
            f"/api/jobs/{urllib.parse.quote(job_id, safe='')}/replay",
            mutating=True,
            idempotency_key=idempotency_key,
        )
        return Job.from_dict(data)

    def job_deliveries(
        self, job_id: str, *, page: Optional[int] = None, page_size: Optional[int] = None
    ) -> DeliveryList:
        """Webhook delivery history of a job, oldest first."""
        data = self._request(
            "GET",
            f"/api/jobs/{urllib.parse.quote(job_id, safe='')}/deliveries",
            query={"page": page, "page_size": page_size},
        )
        return DeliveryList(
            deliveries=[Delivery.from_dict(d) for d in data.get("deliveries", [])],
            page=Page.from_dict(data.get("page", {})),
        )

    def iter_job(self, job_id: str, *, interval: float = 2.0) -> Iterator[Job]:
        """Yield a snapshot whenever the job's status changes, then the
        terminal snapshot, and stop. Polls every ``interval`` seconds."""
        last_status = None
        while True:
            job = self.get_job(job_id)
            if job.status != last_status or job.terminal:
                yield job
                last_status = job.status
            if job.terminal:
                return
            time.sleep(interval)

    def watch_job(self, job_id: str, *, interval: float = 2.0, timeout: Optional[float] = None) -> Job:
        """Poll the job until it reaches a terminal state and return the
        final snapshot. Raises :class:`RavenError` with code
        ``watch_timeout`` when ``timeout`` (seconds) elapses first."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            job = self.get_job(job_id)
            if job.terminal:
                return job
            if deadline is not None and time.monotonic() >= deadline:
                raise RavenError(
                    code="watch_timeout",
                    message=f"job {job_id} did not reach a terminal state in {timeout}s",
                )
            time.sleep(interval)

    # ------------------------------------------------------------------
    # Cron schedules
    # ------------------------------------------------------------------

    def create_cron(
        self,
        name: str,
        cron_expr: str,
        type: str,
        payload: Dict[str, Any],
        *,
        priority: Optional[int] = None,
        enabled: Optional[bool] = None,
        idempotency_key: Optional[str] = None,
    ) -> Cron:
        """Register a recurring schedule. ``cron_expr`` is the classic
        5-field form ``min hour dom month dow``. The response includes the
        computed ``next_run_at``."""
        body: Dict[str, Any] = {
            "name": name,
            "cron_expr": cron_expr,
            "type": type,
            "payload": payload,
        }
        if priority is not None:
            body["priority"] = priority
        if enabled is not None:
            body["enabled"] = enabled
        data = self._request(
            "POST", "/api/crons", body, mutating=True, idempotency_key=idempotency_key
        )
        return Cron.from_dict(data)

    def list_crons(self, *, page: Optional[int] = None, page_size: Optional[int] = None) -> CronList:
        """Page through the caller's schedules."""
        data = self._request("GET", "/api/crons", query={"page": page, "page_size": page_size})
        return CronList(
            crons=[Cron.from_dict(c) for c in data.get("crons", [])],
            page=Page.from_dict(data.get("page", {})),
        )

    def delete_cron(self, cron_id: str, *, idempotency_key: Optional[str] = None) -> None:
        """Hard-delete a schedule: it stops firing immediately. Unknown or
        foreign ids answer ``404 cron_not_found``."""
        self._request(
            "DELETE",
            f"/api/crons/{urllib.parse.quote(cron_id, safe='')}",
            mutating=True,
            idempotency_key=idempotency_key,
        )

    # ------------------------------------------------------------------
    # API keys
    # ------------------------------------------------------------------

    def create_api_key(
        self, name: str, scopes: List[str], *, idempotency_key: Optional[str] = None
    ) -> CreateAPIKeyResult:
        """Mint a new API key. JWT-only route: an API key cannot create more
        keys (``403 api_key_cannot_create_keys``). Save ``result.key``
        immediately — it is shown exactly once."""
        data = self._request(
            "POST",
            "/api/keys",
            {"name": name, "scopes": scopes},
            mutating=True,
            idempotency_key=idempotency_key,
        )
        return CreateAPIKeyResult(key=data["key"], api_key=APIKey.from_dict(data["api_key"]))

    def list_api_keys(self) -> List[APIKey]:
        """The caller's active keys, newest first. Revoked keys disappear;
        the secret hash is never returned."""
        data = self._request("GET", "/api/keys")
        return [APIKey.from_dict(k) for k in data.get("api_keys", [])]

    def revoke_api_key(self, key_id: str, *, idempotency_key: Optional[str] = None) -> None:
        """Soft-revoke a key, effective immediately. A key belonging to
        someone else answers ``404 api_key_not_found`` — no ownership
        oracle."""
        self._request(
            "DELETE",
            f"/api/keys/{urllib.parse.quote(key_id, safe='')}",
            mutating=True,
            idempotency_key=idempotency_key,
        )

    # ------------------------------------------------------------------
    # Ops
    # ------------------------------------------------------------------

    def list_workers(self) -> List[Worker]:
        """The live worker registry, read straight from Redis: every listed
        worker heartbeat within the last 15 seconds."""
        data = self._request("GET", "/api/workers")
        return [Worker.from_dict(w) for w in data.get("workers", [])]

    def health_services(self) -> HealthReport:
        """The aggregated health grid the gateway computes by probing every
        service. Public route."""
        data = self._request("GET", "/api/health/services")
        return HealthReport(
            checked_at=data.get("checked_at", ""),
            services=[ServiceStatus.from_dict(s) for s in data.get("services", [])],
        )

    def list_audit_events(
        self,
        *,
        action: Optional[str] = None,
        actor: Optional[str] = None,
        limit: Optional[int] = None,
        before_id: Optional[int] = None,
    ) -> AuditList:
        """Page the platform audit trail, newest first. Admin-only
        (``users:delete``). ``before_id`` is the keyset cursor from a
        previous page's ``next_before_id``."""
        data = self._request(
            "GET",
            "/api/audit",
            query={"action": action, "actor": actor, "limit": limit, "before_id": before_id},
        )
        return AuditList(
            events=[AuditEvent.from_dict(e) for e in data.get("events", [])],
            next_before_id=data.get("next_before_id"),
        )
