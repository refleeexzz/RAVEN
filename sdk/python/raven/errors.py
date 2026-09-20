"""Typed errors for the RAVEN Python SDK."""

from __future__ import annotations

from typing import Optional


class RavenError(Exception):
    """An API failure, mirroring the gateway's error envelope.

    The gateway answers every error as::

        {"error": {"code": "job_not_found", "message": "job does not exist",
                    "request_id": "0f7b2c..."}}

    Attributes:
        status_code: the HTTP status of the response (0 for transport errors
            that never got a response).
        code: stable snake_case machine string, e.g. ``job_not_found``.
        message: client-safe prose.
        request_id: matches the ``request_id`` field in the gateway's JSON
            logs — quote it when reporting issues.
    """

    def __init__(
        self,
        code: str,
        message: str,
        request_id: Optional[str] = None,
        status_code: int = 0,
    ) -> None:
        self.code = code
        self.message = message
        self.request_id = request_id
        self.status_code = status_code
        super().__init__(str(self))

    def __str__(self) -> str:
        base = f"raven: {self.code}: {self.message}"
        if self.status_code:
            base += f" (status {self.status_code})"
        if self.request_id:
            base += f" [request {self.request_id}]"
        return base

    def __repr__(self) -> str:
        return (
            f"RavenError(code={self.code!r}, message={self.message!r}, "
            f"request_id={self.request_id!r}, status_code={self.status_code!r})"
        )
