"""Thread-safe background job coordination for the local BookPilot web app."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import threading
from typing import Any, Callable, Dict, Optional
from uuid import uuid4


ACTIVE_STATES = {"queued", "running"}


class JobAlreadyRunning(RuntimeError):
    """Raised when a second library update is requested."""

    def __init__(self, job: Dict[str, Any]):
        super().__init__("A library update is already running.")
        self.job = job


class JobManager:
    """Run at most one allowlisted library task in a daemon thread."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._job: Optional[Dict[str, Any]] = None
        self._thread: Optional[threading.Thread] = None

    @staticmethod
    def _timestamp() -> str:
        return datetime.now(timezone.utc).isoformat()

    def snapshot(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            return deepcopy(self._job)

    def is_active(self) -> bool:
        with self._lock:
            return bool(self._job and self._job["state"] in ACTIVE_STATES)

    def start(
        self,
        action: str,
        label: str,
        target: Callable[[Callable[..., None]], Optional[Dict[str, Any]]],
    ) -> Dict[str, Any]:
        """Start ``target`` and return its initial serializable job snapshot."""
        with self._lock:
            if self._job and self._job["state"] in ACTIVE_STATES:
                raise JobAlreadyRunning(deepcopy(self._job))

            job_id = uuid4().hex
            self._job = {
                "id": job_id,
                "action": action,
                "label": label,
                "state": "queued",
                "message": f"Preparing {label.lower()}…",
                "current": 0,
                "total": None,
                "percent": None,
                "created_at": self._timestamp(),
                "started_at": None,
                "completed_at": None,
                "result": None,
                "error": None,
            }

            self._thread = threading.Thread(
                target=self._run,
                args=(job_id, target),
                name=f"bookpilot-{action}-{job_id[:8]}",
                daemon=True,
            )
            self._thread.start()
            return deepcopy(self._job)

    def _run(
        self,
        job_id: str,
        target: Callable[[Callable[..., None]], Optional[Dict[str, Any]]],
    ) -> None:
        self._update(job_id, state="running", started_at=self._timestamp())

        def progress(
            *,
            message: Optional[str] = None,
            current: Optional[int] = None,
            total: Optional[int] = None,
            **details: Any,
        ) -> None:
            changes: Dict[str, Any] = {}
            if message is not None:
                changes["message"] = message
            if current is not None:
                changes["current"] = max(0, int(current))
            if total is not None:
                changes["total"] = max(0, int(total))
            changes.update(details)
            self._update(job_id, **changes)

        try:
            result = target(progress) or {}
            message = result.get("message") or "Library update complete."
            self._update(
                job_id,
                state="succeeded",
                message=message,
                result=result,
                completed_at=self._timestamp(),
            )
        except Exception as exc:
            self._update(
                job_id,
                state="failed",
                message="Library update failed.",
                error=str(exc) or exc.__class__.__name__,
                completed_at=self._timestamp(),
            )

    def _update(self, job_id: str, **changes: Any) -> None:
        with self._lock:
            if not self._job or self._job["id"] != job_id:
                return
            self._job.update(changes)
            current = self._job.get("current")
            total = self._job.get("total")
            if total and current is not None:
                self._job["percent"] = min(100, round((current / total) * 100))
            else:
                self._job["percent"] = None

    def wait(self, timeout: Optional[float] = None) -> None:
        """Wait for the current job; intended for tests and orderly shutdowns."""
        thread = self._thread
        if thread:
            thread.join(timeout)
