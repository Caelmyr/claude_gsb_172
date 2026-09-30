"""JSON and time helpers.

JSON is the wire and storage format for the whole system, so a single module
owns the fiddly bits:

* all timestamps are epoch **milliseconds** (integers) — no datetime objects
  ever reach a JSON document, which keeps serialization trivially reversible;
* floats are sanitised so ``NaN`` / ``Inf`` (which are legal in Python but not
  in strict JSON) never corrupt a file;
* ``ensure_ascii=False`` keeps Chinese and other non-ASCII text human readable.
"""

from __future__ import annotations

import json
import math
import time
from typing import Any


# ---------------------------------------------------------------------------
# Time
# ---------------------------------------------------------------------------
def now_ms() -> int:
    """Current epoch time in milliseconds."""
    return int(time.time() * 1000)


def iso_from_ms(ms: int) -> str:
    """Format epoch-ms as a local ``YYYY-MM-DD HH:MM:SS`` string for display."""
    if not ms:
        return "-"
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ms / 1000))
    except (OverflowError, OSError, ValueError):
        return str(ms)


def ms_since(ms: int) -> int:
    return max(0, now_ms() - ms)


# ---------------------------------------------------------------------------
# Value sanitation
# ---------------------------------------------------------------------------
def _sanitize_scalar(value: Any) -> Any:
    if isinstance(value, float):
        if math.isnan(value):
            return None
        if math.isinf(value):
            return "inf" if value > 0 else "-inf"
    return value


def sanitize(value: Any) -> Any:
    """Recursively replace NaN/Inf floats so the value is strict-JSON safe."""
    if isinstance(value, dict):
        return {str(k): sanitize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [sanitize(v) for v in value]
    return _sanitize_scalar(value)


# ---------------------------------------------------------------------------
# Dump / load
# ---------------------------------------------------------------------------
def dumps(obj: Any, pretty: bool = True) -> str:
    """Serialize to a JSON string (UTF-8, no ASCII escaping, NaN-safe)."""
    kwargs: dict = {"ensure_ascii": False}
    if pretty:
        kwargs["indent"] = 2
    return json.dumps(sanitize(obj), default=str, **kwargs)


def loads(text: str | bytes, default: Any = None) -> Any:
    """Tolerant JSON parse; returns ``default`` on any failure."""
    try:
        return json.loads(text)
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
# JSON-lines helpers
# ---------------------------------------------------------------------------
def dumps_line(obj: Any) -> str:
    """One JSON object on a single line (for JSONL log / shuffle files)."""
    return json.dumps(sanitize(obj), ensure_ascii=False, default=str)


def parse_line(line: str, default: Any = None) -> Any:
    """Parse a single JSONL line, skipping blanks."""
    line = line.strip()
    if not line:
        return default
    try:
        return json.loads(line)
    except (ValueError, TypeError):
        return default
