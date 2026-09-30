"""HTTP client with timeout, retries and exponential backoff.

Nodes communicate exclusively over HTTP, so a resilient client is the backbone
of distributed coordination: transient connection errors, a Worker briefly
restarting, or a busy Master must not abort a task dispatch.  Every request is
retried with jittered exponential backoff until a deadline is reached.
"""

from __future__ import annotations

import json
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Optional

from .jsonutil import now_ms


@dataclass
class HttpResponse:
    status: int
    data: Any          # decoded JSON when possible, else raw text
    headers: dict = None
    elapsed_ms: int = 0

    def __post_init__(self):
        if self.headers is None:
            self.headers = {}

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300


class HttpError(RuntimeError):
    """Raised when a request fails permanently after all retries."""

    def __init__(self, message: str, status: int = 0):
        super().__init__(message)
        self.status = status


class HttpClient:
    """Small urllib-backed client.

    ``urllib`` is used (rather than a third-party dependency) so the framework
    has no runtime requirements beyond the standard library and Flask.
    """

    def __init__(
        self,
        timeout: float = 8.0,
        retries: int = 3,
        backoff_base: float = 0.4,
        max_backoff: float = 4.0,
    ) -> None:
        self.timeout = timeout
        self.retries = retries
        self.backoff_base = backoff_base
        self.max_backoff = max_backoff

    # ------------------------------------------------------------------
    def _build_url(self, url: str, params: Optional[dict] = None) -> str:
        if params:
            sep = "&" if "?" in url else "?"
            url += sep + urllib.parse.urlencode(params)
        return url

    def _open(self, req: urllib.request.Request, deadline: float) -> HttpResponse:
        last_err: Optional[Exception] = None
        attempt = 0
        while True:
            attempt += 1
            try:
                started = now_ms()
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    body = resp.read()
                    elapsed = now_ms() - started
                    return self._decode(resp.status, body, dict(resp.headers), elapsed)
            except urllib.error.HTTPError as e:
                # An HTTP error status is a definitive server answer — read its
                # body and return it rather than retrying on 4xx/5xx blindly.
                try:
                    body = e.read()
                except Exception:
                    body = b""
                return self._decode(e.code, body, dict(e.headers) if e.headers else {}, 0)
            except (urllib.error.URLError, socket.timeout, TimeoutError, ConnectionError, OSError) as e:
                last_err = e
            if time.monotonic() >= deadline or attempt > self.retries:
                break
            # Jittered exponential backoff so a fleet of nodes doesn't retry in lockstep.
            delay = min(self.max_backoff, self.backoff_base * (2 ** (attempt - 1)))
            delay *= 0.5 + 0.5 * ((time.time_ns() % 1000) / 1000.0)
            time.sleep(delay)

        raise HttpError(f"request failed after {attempt} attempts: {last_err}", status=0)

    @staticmethod
    def _decode(status: int, body: bytes, headers: dict, elapsed: int) -> HttpResponse:
        text = body.decode("utf-8", errors="replace")
        ctype = headers.get("Content-Type", "")
        if "application/json" in ctype or text.lstrip().startswith(("{", "[")):
            data = json.loads(text) if text.strip() else None
        else:
            data = text
        return HttpResponse(status=status, data=data, headers=headers, elapsed_ms=elapsed)

    # ------------------------------------------------------------------
    def request(
        self,
        method: str,
        url: str,
        json_body: Any = None,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        timeout: Optional[float] = None,
    ) -> HttpResponse:
        url = self._build_url(url, params)
        data_bytes: Optional[bytes] = None
        hdrs = dict(headers or {})
        if json_body is not None:
            data_bytes = json.dumps(json_body, ensure_ascii=False).encode("utf-8")
            hdrs.setdefault("Content-Type", "application/json")
        req = urllib.request.Request(url, data=data_bytes, headers=hdrs, method=method)
        deadline = time.monotonic() + (timeout or self.timeout) * (self.retries + 1)
        return self._open(req, deadline)

    def get(self, url: str, params: Optional[dict] = None, timeout: Optional[float] = None) -> HttpResponse:
        return self.request("GET", url, params=params, timeout=timeout)

    def post(self, url: str, json_body: Any = None, timeout: Optional[float] = None) -> HttpResponse:
        return self.request("POST", url, json_body=json_body, timeout=timeout)

    def put(self, url: str, json_body: Any = None, timeout: Optional[float] = None) -> HttpResponse:
        return self.request("PUT", url, json_body=json_body, timeout=timeout)

    def delete(self, url: str, timeout: Optional[float] = None) -> HttpResponse:
        return self.request("DELETE", url, timeout=timeout)

    # Convenience helpers returning decoded data directly.
    def get_json(self, url: str, params: Optional[dict] = None, default: Any = None) -> Any:
        try:
            resp = self.get(url, params=params)
            return resp.data if resp.ok else default
        except HttpError:
            return default

    def post_json(self, url: str, body: Any, default: Any = None) -> Any:
        try:
            resp = self.post(url, json_body=body)
            return resp.data if resp.ok else default
        except HttpError:
            return default
