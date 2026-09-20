"""Dataclass models for the RAVEN Python SDK.

Field names match the gateway's JSON wire format one-to-one. Timestamps on
jobs, crons and deliveries are Unix seconds (0 means "not set"); API keys
and workers report ISO-8601 strings, exactly as the API returns them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

TERMINAL_STATUSES = frozenset({"SUCCESS", "FAILED", "CANCELLED", "DEAD"})


@dataclass
class TokenPair:
    """The credential bundle returned by login and refresh.

    ``access_expires_at`` / ``refresh_expires_at`` are Unix seconds.
    """

    access_token: str
    refresh_token: str
    access_expires_at: int
    refresh_expires_at: int

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "TokenPair":
        return cls(
            access_token=d["access_token"],
            refresh_token=d["refresh_token"],
            access_expires_at=int(d["access_expires_at"]),
            refresh_expires_at=int(d["refresh_expires_at"]),
        )


@dataclass
class Job:
    """One unit of work on the platform."""

    id: str
    type: str
    payload: Dict[str, Any]
    status: str
    priority: int = 5
    attempts: int = 0
    max_attempts: int = 4
    created_at: int = 0
    started_at: int = 0
    finished_at: int = 0
    error: str = ""
    worker_id: str = ""
    scheduled_at: int = 0
    replayed_from: Optional[str] = None

    @property
    def terminal(self) -> bool:
        """True when the job will never leave its current state on its own."""
        return self.status in TERMINAL_STATUSES

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Job":
        return cls(
            id=d["id"],
            type=d.get("type", ""),
            payload=d.get("payload") or {},
            status=d.get("status", ""),
            priority=int(d.get("priority", 5)),
            attempts=int(d.get("attempts", 0)),
            max_attempts=int(d.get("max_attempts", 4)),
            created_at=int(d.get("created_at", 0)),
            started_at=int(d.get("started_at", 0)),
            finished_at=int(d.get("finished_at", 0)),
            error=d.get("error", ""),
            worker_id=d.get("worker_id", ""),
            scheduled_at=int(d.get("scheduled_at", 0)),
            replayed_from=d.get("replayed_from"),
        )


@dataclass
class Page:
    """The pagination envelope returned by every list endpoint."""

    page: int
    page_size: int
    total: int

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Page":
        return cls(
            page=int(d.get("page", 1)),
            page_size=int(d.get("page_size", 20)),
            total=int(d.get("total", 0)),
        )


@dataclass
class JobList:
    jobs: List[Job]
    page: Page


@dataclass
class Delivery:
    """One webhook delivery attempt recorded for a job.

    ``status_code`` and ``latency_ms`` are ``None`` when no response ever
    came back (transport errors and egress-guard refusals).
    """

    id: int
    job_id: str
    attempt: int
    url: str
    ts: int
    status_code: Optional[int] = None
    latency_ms: Optional[int] = None
    response_snippet: str = ""
    blocked: bool = False
    error: str = ""

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Delivery":
        return cls(
            id=int(d["id"]),
            job_id=d.get("job_id", ""),
            attempt=int(d.get("attempt", 0)),
            url=d.get("url", ""),
            ts=int(d.get("ts", 0)),
            status_code=d.get("status_code"),
            latency_ms=d.get("latency_ms"),
            response_snippet=d.get("response_snippet", ""),
            blocked=bool(d.get("blocked", False)),
            error=d.get("error", ""),
        )


@dataclass
class DeliveryList:
    deliveries: List[Delivery]
    page: Page


@dataclass
class Cron:
    """A recurring job schedule. Times are Unix seconds."""

    id: str
    name: str
    cron_expr: str
    type: str
    payload: Dict[str, Any]
    priority: int = 5
    enabled: bool = True
    next_run_at: int = 0
    last_run_at: int = 0
    created_at: int = 0

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Cron":
        return cls(
            id=d["id"],
            name=d.get("name", ""),
            cron_expr=d.get("cron_expr", ""),
            type=d.get("type", ""),
            payload=d.get("payload") or {},
            priority=int(d.get("priority", 5)),
            enabled=bool(d.get("enabled", True)),
            next_run_at=int(d.get("next_run_at", 0)),
            last_run_at=int(d.get("last_run_at", 0)),
            created_at=int(d.get("created_at", 0)),
        )


@dataclass
class CronList:
    crons: List[Cron]
    page: Page


@dataclass
class APIKey:
    """API key metadata. The secret itself is only shown once, at creation."""

    id: str
    name: str
    prefix: str
    scopes: List[str] = field(default_factory=list)
    created_at: str = ""
    last_used_at: Optional[str] = None

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "APIKey":
        return cls(
            id=d["id"],
            name=d.get("name", ""),
            prefix=d.get("prefix", ""),
            scopes=list(d.get("scopes") or []),
            created_at=d.get("created_at", ""),
            last_used_at=d.get("last_used_at"),
        )


@dataclass
class CreateAPIKeyResult:
    """The one and only time the raw key material is shown. Store it."""

    key: str
    api_key: APIKey


@dataclass
class Worker:
    """One live worker from the Redis registry. Counters arrive as strings."""

    id: str
    started_at: str = ""
    last_heartbeat: str = ""
    jobs_processed: str = "0"
    in_flight: str = "0"

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Worker":
        return cls(
            id=d["id"],
            started_at=d.get("started_at", ""),
            last_heartbeat=d.get("last_heartbeat", ""),
            jobs_processed=str(d.get("jobs_processed", "0")),
            in_flight=str(d.get("in_flight", "0")),
        )


@dataclass
class ServiceStatus:
    """One row of the aggregated health grid."""

    name: str
    status: str  # ok | degraded | down
    latency_ms: int = 0
    detail: str = ""

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ServiceStatus":
        return cls(
            name=d["name"],
            status=d.get("status", ""),
            latency_ms=int(d.get("latency_ms", 0)),
            detail=d.get("detail", ""),
        )


@dataclass
class HealthReport:
    checked_at: str
    services: List[ServiceStatus]


@dataclass
class AuditEvent:
    """One row of the platform audit trail (admin only)."""

    ts: str
    actor_id: str
    action: str
    outcome: str
    id: Optional[int] = None
    resource_type: str = ""
    resource_id: str = ""
    ip: str = ""
    user_agent: str = ""
    trace_id: str = ""
    detail: Optional[Dict[str, Any]] = None

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "AuditEvent":
        return cls(
            id=d.get("id"),
            ts=d.get("ts", ""),
            actor_id=d.get("actor_id", ""),
            action=d.get("action", ""),
            outcome=d.get("outcome", ""),
            resource_type=d.get("resource_type", ""),
            resource_id=d.get("resource_id", ""),
            ip=d.get("ip", ""),
            user_agent=d.get("user_agent", ""),
            trace_id=d.get("trace_id", ""),
            detail=d.get("detail"),
        )


@dataclass
class AuditList:
    """Audit page. ``next_before_id`` is the keyset cursor for the next
    (older) page; ``None`` means the end of the trail."""

    events: List[AuditEvent]
    next_before_id: Optional[int] = None
