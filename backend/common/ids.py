"""Identifier generation.

Identifiers are short, time-ordered and collision-resistant so they can be used
directly as file names without any coordination between processes (an important
property in a distributed system where two Workers may mint IDs concurrently).
"""

from __future__ import annotations

import random
import string
import time

_HEX = string.hexdigits.lower()


def _rand_hex(length: int) -> str:
    # ``random`` is seeded from the OS by default; for IDs a non-crypto RNG is
    # sufficient because we also mix in a high-resolution timestamp.
    return "".join(random.choice(_HEX) for _ in range(length))


def new_id(prefix: str, rand_len: int = 6) -> str:
    """Return ``prefix-<hex-epoch-ms>-<random>``.

    The millisecond timestamp keeps IDs sortable by creation time while the
    random suffix prevents collisions between nodes creating IDs in the same
    millisecond.
    """
    ts = format(int(time.time() * 1000), "x")
    return f"{prefix}-{ts}-{_rand_hex(rand_len)}"


def task_id(kind: str, index: int) -> str:
    """Deterministic task id: ``m-0003`` for the 4th map task, ``r-0001`` for a reduce."""
    return f"{kind}-{index:04d}"


def shard_id(kind: str, index: int) -> str:
    """Deterministic shard id: ``in-0002``, ``map-0002``, ``red-0000``."""
    return f"{kind}-{index:04d}"


def partition_name(index: int) -> str:
    """Name of a shuffle / result partition."""
    return f"part-{index:04d}"


def parse_index(identifier: str) -> int:
    """Extract the zero-padded numeric suffix from an id (``m-0003`` -> 3)."""
    return int(identifier.rsplit("-", 1)[1])
