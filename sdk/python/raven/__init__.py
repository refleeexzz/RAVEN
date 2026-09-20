"""RAVEN Python SDK — official client for the RAVEN distributed jobs platform.

Talks to the public REST API exposed by the gateway
(default ``http://localhost:8080``). Zero dependencies: standard library
only (``urllib.request``).

Quick start::

    from raven import Client

    client = Client(base_url="http://localhost:8080")
    client.login("me@example.com", "correct horse battery")

    job = client.create_job(type="webhook", payload={"url": "https://me.example/hook"})
    final = client.watch_job(job.id)
    print(final.status)

For machine credentials use an API key instead::

    client = Client(api_key="rav_live_...")

Every API failure raises :class:`RavenError` with the stable machine
``code``, the client-safe ``message`` and the ``request_id`` that matches
the gateway logs.
"""

from .client import Client
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

__version__ = "1.0.0"

__all__ = [
    "APIKey",
    "AuditEvent",
    "AuditList",
    "Client",
    "CreateAPIKeyResult",
    "Cron",
    "CronList",
    "Delivery",
    "DeliveryList",
    "HealthReport",
    "Job",
    "JobList",
    "Page",
    "RavenError",
    "ServiceStatus",
    "TokenPair",
    "Worker",
    "__version__",
]
