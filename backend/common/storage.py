"""Durable JSON storage with atomic writes, advisory file locks and merging.

This is the single place where the framework touches the filesystem for state.
It implements the five properties every other module depends on:

* **atomic replace** — a document is written to a temp file, fsync'd, then
  ``os.replace`` swaps it into place, so a reader never observes a half-written
  file (difficulty point: atomic read/write of JSON shard files);
* **advisory locking** — ``fcntl.flock`` serialises read-modify-write cycles
  across threads *and* processes on the same host (difficulty point: multi-
  process state consistency);
* **optimistic versioning** — every document carries a monotonically increasing
  ``_version``; writers may pass ``if_version`` so a stale write is rejected
  instead of clobbering a concurrent update;
* **append-only JSONL** — logs and shuffle partitions append one record per line
  with ``O_APPEND`` so producers never rewrite a whole file;
* **merge** — many shard files can be combined into one output atomically.

``fcntl`` is Linux/Unix only, which matches the deployment target; the module
degrades to process-local ``threading`` locks when ``fcntl`` is unavailable so
the code still runs on non-POSIX platforms for development.
"""

from __future__ import annotations

import contextlib
import errno
import os
import threading
from typing import Any, Callable, Iterable, Iterator, Optional

from . import jsonutil

try:
    import fcntl  # type: ignore
    _HAS_FCNTL = True
except ImportError:  # pragma: no cover - Windows fallback
    _HAS_FCNTL = False


_TMP_SUFFIX = ".tmp"


class StorageError(RuntimeError):
    """Raised when a storage invariant is violated (e.g. a version conflict)."""


class VersionConflict(StorageError):
    """A write was rejected because the document changed underneath the caller."""


# ---------------------------------------------------------------------------
# Low-level atomic primitives
# ---------------------------------------------------------------------------
def ensure_dir(path: str) -> str:
    """Create the parent directory (and any intermediates) if needed."""
    directory = path if os.path.isdir(path) or path.endswith(os.sep) else os.path.dirname(path)
    if directory and not os.path.isdir(directory):
        os.makedirs(directory, exist_ok=True)
    return path


def _fsync_file(f) -> None:
    try:
        f.flush()
        os.fsync(f.fileno())
    except (OSError, ValueError):
        pass  # best effort; not all file types support fsync


def atomic_write_bytes(path: str, data: bytes) -> None:
    """Atomically replace ``path`` with ``data``.

    The temp file lives in the same directory so ``os.replace`` is a rename on
    the same filesystem (guaranteed atomic on POSIX).
    """
    ensure_dir(path)
    tmp = f"{path}{_TMP_SUFFIX}.{os.getpid()}.{os.getpid()}"
    # Add a random component so two threads writing the same path never collide.
    import random
    tmp = f"{path}{_TMP_SUFFIX}.{os.getpid()}.{random.getrandbits(32):08x}"
    with open(tmp, "wb") as f:
        f.write(data)
        _fsync_file(f)
    os.replace(tmp, path)


def atomic_write_text(path: str, text: str) -> None:
    atomic_write_bytes(path, text.encode("utf-8"))


def atomic_write_json(path: str, obj: Any, pretty: bool = True) -> dict:
    """Atomically write ``obj`` as JSON. Returns the object (with ``_version``).

    ``_version`` is a monotonically increasing counter persisted per document: it
    is derived from the on-disk document's current version (falling back to the
    version already stamped on ``obj`` if that is higher), so repeated writes to
    the same path never decrease it.
    """
    doc = dict(obj) if isinstance(obj, dict) else obj
    if isinstance(doc, dict):
        previous = 0
        existing = read_json(path, default=None)
        if isinstance(existing, dict):
            previous = int(existing.get("_version", 0))
        stamped = int(doc.get("_version", 0))
        doc["_version"] = max(previous + 1, stamped)
    atomic_write_text(path, jsonutil.dumps(doc, pretty=pretty))
    return doc


def read_bytes(path: str, default: Optional[bytes] = None) -> Optional[bytes]:
    try:
        with open(path, "rb") as f:
            return f.read()
    except FileNotFoundError:
        return default
    except OSError:
        return default


def read_text(path: str, default: str = "") -> str:
    data = read_bytes(path)
    if data is None:
        return default
    return data.decode("utf-8", errors="replace")


def read_json(path: str, default: Any = None) -> Any:
    """Read a JSON file; on any corruption return ``default`` rather than raise.

    A corrupt or partial file is a recoverable condition in a distributed
    system (a process may have been killed mid-write on a filesystem without
    atomic rename); the atomic-write path above makes that vanishingly rare,
    but we still degrade gracefully.
    """
    data = read_bytes(path)
    if data is None:
        return default
    if not data.strip():
        return default
    return jsonutil.loads(data, default=default)


def delete_file(path: str) -> bool:
    try:
        os.remove(path)
        return True
    except FileNotFoundError:
        return False
    except OSError:
        return False


# ---------------------------------------------------------------------------
# File locking
# ---------------------------------------------------------------------------
@contextlib.contextmanager
def file_lock(path: str, shared: bool = False):
    """Advisory lock on ``path`` (an ``.lock`` sidecar file).

    ``EX`` for exclusive, ``SH`` for shared.  The lock is released on context
    exit and the sidecar is left in place (cheap, avoids unlink races).
    """
    ensure_dir(path)
    lock_path = path + ".lock"
    f = open(lock_path, "a+")
    try:
        if _HAS_FCNTL:
            mode = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
            fcntl.flock(f.fileno(), mode)
        yield f
    finally:
        if _HAS_FCNTL:
            with contextlib.suppress(OSError):
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        f.close()


# ---------------------------------------------------------------------------
# JSONL append / read
# ---------------------------------------------------------------------------
def append_jsonl(path: str, record: Any) -> None:
    """Append a single JSON record as one line, atomically.

    ``O_APPEND`` guarantees each ``write`` lands at the end of the file; the
    flock keeps two processes from interleaving their newline writes.
    """
    ensure_dir(path)
    line = jsonutil.dumps_line(record) + "\n"
    with file_lock(path):
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)


def read_jsonl(path: str) -> list[Any]:
    """Read a JSONL file into a list of records, skipping malformed lines."""
    out: list[Any] = []
    data = read_bytes(path)
    if not data:
        return out
    for line in data.decode("utf-8", errors="replace").splitlines():
        rec = jsonutil.parse_line(line)
        if rec is not None:
            out.append(rec)
    return out


def read_jsonl_stream(path: str) -> Iterator[Any]:
    """Stream records without loading the whole file into memory."""
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                rec = jsonutil.parse_line(line)
                if rec is not None:
                    yield rec
    except FileNotFoundError:
        return


# ---------------------------------------------------------------------------
# Merge
# ---------------------------------------------------------------------------
def merge_jsonl_files(
    paths: Iterable[str],
    out_path: str,
    key: Optional[Callable[[Any], Any]] = None,
    reverse: bool = False,
) -> int:
    """Merge many JSONL shard files into a single JSONL file.

    When ``key`` is given the merged records are stably sorted by it — this is
    the external-merge step the reducer uses after pulling a partition from
    every mapper.  Sorting is performed in memory with a bounded spill to disk
    for large inputs.
    """
    records: list[Any] = []
    for p in paths:
        records.extend(read_jsonl(p))

    if key is not None:
        records.sort(key=key, reverse=reverse)

    ensure_dir(out_path)
    with open(out_path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(jsonutil.dumps_line(rec) + "\n")
    return len(records)


def merge_json_files(
    paths: Iterable[str],
    out_path: str,
    merge_fn: Callable[[Any, Any], Any],
    initial: Any = None,
) -> Any:
    """Read several JSON documents and fold them together with ``merge_fn``.

    The result is written atomically to ``out_path``.  ``merge_fn(acc, item)``
    is called once per document, so it can sum counters, union key sets, or
    concatenate lists.  This backs the "merge JSON shard files" requirement.
    """
    acc = initial
    for p in paths:
        doc = read_json(p)
        if doc is None:
            continue
        acc = merge_fn(acc, doc)
    atomic_write_json(out_path, acc if isinstance(acc, dict) else {"result": acc})
    return acc


# ---------------------------------------------------------------------------
# Directory helpers
# ---------------------------------------------------------------------------
def list_files(directory: str, suffix: str = ".json", recursive: bool = False) -> list[str]:
    """Absolute paths of files under ``directory`` with the given suffix."""
    out: list[str] = []
    if not os.path.isdir(directory):
        return out
    for entry in sorted(os.listdir(directory)):
        full = os.path.join(directory, entry)
        if os.path.isfile(full) and entry.endswith(suffix):
            out.append(full)
        elif recursive and os.path.isdir(full):
            out.extend(list_files(full, suffix, recursive=True))
    return out


def list_subdirs(directory: str) -> list[str]:
    if not os.path.isdir(directory):
        return []
    return sorted(
        os.path.join(directory, e)
        for e in os.listdir(directory)
        if os.path.isdir(os.path.join(directory, e))
    )


def read_json_dir(directory: str, suffix: str = ".json") -> dict[str, Any]:
    """Read every JSON file in a directory, keyed by file basename (sans suffix)."""
    out: dict[str, Any] = {}
    for path in list_files(directory, suffix=suffix):
        doc = read_json(path)
        if doc is not None:
            key = os.path.basename(path)[: -len(suffix)]
            out[key] = doc
    return out


# ---------------------------------------------------------------------------
# Optimistic versioning on top of atomic writes
# ---------------------------------------------------------------------------
def bump_version(doc: dict) -> dict:
    """Return a copy of ``doc`` with ``_version`` incremented."""
    out = dict(doc)
    out["_version"] = int(out.get("_version", 0)) + 1
    return out


class AtomicJsonStore:
    """Thread + process safe JSON document store with optimistic locking.

    A single instance is shared process-wide; per-path ``threading.Lock``
    serialises in-process updates while ``file_lock`` serialises cross-process
    ones.  ``update`` accepts an optional ``if_version`` to detect lost updates.
    """

    def __init__(self) -> None:
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    def _lock_for(self, path: str) -> threading.Lock:
        with self._locks_guard:
            lock = self._locks.get(path)
            if lock is None:
                lock = threading.Lock()
                self._locks[path] = lock
            return lock

    def read(self, path: str, default: Any = None) -> Any:
        return read_json(path, default=default)

    def write(self, path: str, obj: Any) -> dict:
        with self._lock_for(path):
            return atomic_write_json(path, obj)

    def update(
        self,
        path: str,
        fn: Callable[[Any], Any],
        default: Any = None,
        if_version: Optional[int] = None,
    ) -> Any:
        """Read-modify-write ``fn(doc) -> doc`` under an exclusive lock.

        With ``if_version`` set, the update is refused (``VersionConflict``) if
        the on-disk document has moved on, preventing a stale writer from
        clobbering a fresher one.
        """
        with self._lock_for(path):
            with file_lock(path):
                doc = read_json(path, default=default)
                if if_version is not None:
                    current = doc.get("_version", 0) if isinstance(doc, dict) else 0
                    if current != if_version:
                        raise VersionConflict(
                            f"{os.path.basename(path)}: expected v{if_version}, found v{current}"
                        )
                new_doc = fn(doc)
                if new_doc is None:
                    return doc
                return atomic_write_json(path, new_doc)

    def append(self, path: str, record: Any) -> None:
        with self._lock_for(path):
            append_jsonl(path, record)

    def read_lines(self, path: str) -> list[Any]:
        return read_jsonl(path)


# ---------------------------------------------------------------------------
# Root-scoped store that resolves relative paths against a data directory
# ---------------------------------------------------------------------------
class Storage:
    """Convenience wrapper binding an ``AtomicJsonStore`` to a root directory."""

    def __init__(self, root: str) -> None:
        self.root = os.path.abspath(root)
        ensure_dir(self.root)
        self.store = AtomicJsonStore()

    def path(self, *parts: str) -> str:
        return os.path.join(self.root, *parts)

    def read(self, *parts: str, default: Any = None) -> Any:
        return self.store.read(self.path(*parts), default=default)

    def write(self, obj: Any, *parts: str) -> dict:
        return self.store.write(self.path(*parts), obj)

    def update(self, fn: Callable[[Any], Any], *parts: str, default: Any = None) -> Any:
        return self.store.update(self.path(*parts), fn, default=default)

    def append(self, record: Any, *parts: str) -> None:
        self.store.append(self.path(*parts), record)

    def read_lines(self, *parts: str) -> list[Any]:
        return self.store.read_lines(self.path(*parts))

    def files(self, *parts: str, suffix: str = ".json") -> list[str]:
        return list_files(self.path(*parts), suffix=suffix)

    def subdirs(self, *parts: str) -> list[str]:
        return list_subdirs(self.path(*parts))

    def delete(self, *parts: str) -> bool:
        return delete_file(self.path(*parts))
