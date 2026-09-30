"""Built-in map and reduce function library.

These are the "user functions" a job references by name.  The contract is:

* ``mapper(records, params) -> list[(key, value)]`` — stateless, deterministic,
  and *pure* so a subprocess can execute it without any shared state;
* ``reducer(key, values, params) -> dict`` — folds the values for one key into a
  single result record.

Because these functions are looked up by name (see ``tasks.registry``) the
Master never serialises code — it only ships a function name plus data.
"""

from __future__ import annotations

import re
from collections import Counter
from typing import Any, Iterable

# ``re`` split on anything that is not an ASCII letter/digit or a CJK character,
# so English and Chinese word counts both work out of the box.
_WORD_RE = re.compile(r"[^a-zA-Z0-9一-鿿]+")


def _as_text(record: Any) -> str:
    if isinstance(record, dict):
        return str(record.get("text", record.get("line", "")))
    return str(record)


def _words(record: Any) -> Iterable[str]:
    text = _as_text(record)
    return (w for w in _WORD_RE.split(text.lower()) if w)


# ---------------------------------------------------------------------------
# WordCount
# ---------------------------------------------------------------------------
def wordcount_mapper(records: list[Any], params: dict) -> list[tuple[str, Any]]:
    """Emit ``(word, 1)`` for every token in every input record."""
    out: list[tuple[str, Any]] = []
    append = out.append
    for rec in records:
        for word in _words(rec):
            append((word, 1))
    return out


def count_reducer(key: str, values: list[Any], params: dict) -> dict:
    """Sum the per-word counts. Used by wordcount and grep."""
    return {"key": key, "count": sum(values)}


# ---------------------------------------------------------------------------
# Word-length statistics
# ---------------------------------------------------------------------------
def word_length_mapper(records: list[Any], params: dict) -> list[tuple[str, Any]]:
    """Emit ``(word, len(word))`` for aggregate length statistics."""
    out: list[tuple[str, Any]] = []
    append = out.append
    for rec in records:
        for word in _words(rec):
            append((word, len(word)))
    return out


def stats_reducer(key: str, values: list[Any], params: dict) -> dict:
    """count / sum / avg over numeric values."""
    vals = list(values)
    n = len(vals)
    total = sum(vals) if vals else 0
    return {
        "key": key,
        "count": n,
        "sum": total,
        "avg": round(total / n, 3) if n else 0,
    }


# ---------------------------------------------------------------------------
# Grep / filter
# ---------------------------------------------------------------------------
def grep_mapper(records: list[Any], params: dict) -> list[tuple[str, Any]]:
    """Emit ``(pattern, 1)`` for every record containing ``params['pattern']``."""
    pattern = str(params.get("pattern", "map")).lower()
    out: list[tuple[str, Any]] = []
    append = out.append
    for rec in records:
        if pattern in _as_text(rec).lower():
            append((pattern, 1))
    return out


# ---------------------------------------------------------------------------
# Key-value numeric aggregation
# ---------------------------------------------------------------------------
def kv_mapper(records: list[Any], params: dict) -> list[tuple[str, Any]]:
    """Emit ``(record['key'], record['value'])`` from dict-shaped records."""
    out: list[tuple[str, Any]] = []
    append = out.append
    for rec in records:
        if isinstance(rec, dict):
            key = str(rec.get("key", "unknown"))
            value = rec.get("value", 0)
            try:
                value = float(value)
            except (TypeError, ValueError):
                value = 0
            append((key, value))
    return out


def sum_reducer(key: str, values: list[Any], params: dict) -> dict:
    """Sum plus count/avg for numeric key-value aggregation."""
    vals = [float(v) for v in values]
    n = len(vals)
    total = sum(vals) if vals else 0.0
    return {
        "key": key,
        "count": n,
        "sum": round(total, 3),
        "avg": round(total / n, 3) if n else 0.0,
    }


# ---------------------------------------------------------------------------
# Distinct / unique key enumeration
# ---------------------------------------------------------------------------
def distinct_mapper(records: list[Any], params: dict) -> list[tuple[str, Any]]:
    """Emit ``(word, 1)`` once per unique word per record (set semantics)."""
    out: list[tuple[str, Any]] = []
    append = out.append
    for rec in records:
        for word in set(_words(rec)):
            append((word, 1))
    return out
