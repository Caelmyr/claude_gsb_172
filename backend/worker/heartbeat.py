"""Worker heartbeat: periodic liveness + resource report to the Master.

The heartbeat is a plain HTTP POST carrying the worker id and a live resource
sample.  The Master treats a heartbeat as proof-of-life and reaps any worker it
has not heard from within ``heartbeat_timeout_sec`` (see ``master.registry``).
"""

from __future__ import annotations

import threading
from typing import Callable, Optional

from backend.common.http_client import HttpClient


class HeartbeatThread(threading.Thread):
    """Daemon thread that POSTs a heartbeat on a fixed cadence."""

    def __init__(
        self,
        worker_id: str,
        master_url: str,
        interval_sec: float,
        status_provider: Callable[[], dict],
        client: Optional[HttpClient] = None,
    ) -> None:
        super().__init__(daemon=True, name=f"heartbeat-{worker_id}")
        self.worker_id = worker_id
        self.master_url = master_url.rstrip("/")
        self.interval = max(0.2, interval_sec / 4.0)
        self.status_provider = status_provider
        self.client = client or HttpClient(timeout=5.0, retries=1)
        # NOTE: named ``_stop_event`` (not ``_stop``) because ``threading._after_fork``
        # calls the ``Thread._stop()`` method during fork; shadowing it with an Event
        # would raise inside the forked child.
        self._stop_event = threading.Event()

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                payload: dict = {"worker_id": self.worker_id}
                payload.update(self.status_provider())
                self.client.post(f"{self.master_url}/api/workers/heartbeat", payload, timeout=5.0)
            except Exception:
                # A dropped heartbeat is expected during a Master restart; the
                # next tick retries and the Master's own timeout is generous.
                pass
            self._stop_event.wait(self.interval)

    def stop(self) -> None:
        self._stop_event.set()
