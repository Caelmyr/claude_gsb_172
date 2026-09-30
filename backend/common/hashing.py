"""Deterministic hashing for shuffle partitioning.

Python's built-in ``hash()`` is salted per process (``PYTHONHASHSEED``), so two
map tasks running in separate subprocesses would hash the same key differently
and send it to *different* partitions — silently corrupting reduce results.
Shuffle partitioning must be stable across processes and across runs, so we use
a salted MD5 digest instead (deterministic, uniformly distributed, and immune to
hash-collision DoS on adversarial keys).
"""

from __future__ import annotations

import hashlib
from typing import Any

_PARTITION_SALT = b"gsb-mapreduce-shuffle-v1"


def stable_hash(key: Any) -> int:
    """Return a 64-bit deterministic integer hash for an arbitrary key."""
    if isinstance(key, bytes):
        raw = key
    elif isinstance(key, int):
        raw = str(key).encode("utf-8")
    elif isinstance(key, float):
        raw = repr(key).encode("utf-8")
    else:
        raw = str(key).encode("utf-8")
    digest = hashlib.md5(_PARTITION_SALT + raw).digest()
    return int.from_bytes(digest[:8], "big")


def partition_for(key: Any, num_partitions: int) -> int:
    """Map a key to ``0 <= partition < num_partitions`` deterministically."""
    if num_partitions <= 1:
        return 0
    return stable_hash(key) % num_partitions
