"""Task execution with thread and process backends.

A map/reduce task is dispatched to the worker as a plain JSON ``spec`` carrying
the function *names* plus the input records (map) or a fetch plan (reduce).  The
worker executes it in one of two ways:

* ``ThreadBackend`` — runs the task in a daemon thread, reporting progress
  directly.  Used for debugging and for tiny clusters.
* ``ProcessBackend`` (default) — forks a ``multiprocessing`` child that performs
  the real CPU work.  The child communicates with the parent **only** through
  atomic JSON files (``progress.json`` / ``result.json``); the parent polls those
  files and forwards progress to the Master.  This is what makes multi-process
  state consistency a first-class property: there is no shared mutable memory,
  every cross-process boundary is a durable, atomically-written JSON document.

The actual map/reduce algorithm is identical in both modes (``_execute_task``).
"""

from __future__ import annotations

import multiprocessing
import os
import threading
import time
import traceback
from typing import Any, Callable, Optional

from backend.common import constants as C
from backend.common.hashing import partition_for
from backend.common.http_client import HttpClient
from backend.common.jsonutil import now_ms
from backend.common.storage import atomic_write_json, read_json
from backend.tasks.registry import get_mapper, get_reducer
from backend.worker.shuffle_store import SpillSorter, ShuffleStore, partition_filename

ProgressCallback = Callable[[float, int, int], None]


# ---------------------------------------------------------------------------
# Core algorithm (runs identically in a thread or a subprocess)
# ---------------------------------------------------------------------------
def _run_map(spec: dict, data_root: str, progress_cb: ProgressCallback) -> dict:
    """Run a map task: mapper(records) -> hash-partitioned shuffle files."""
    mapper = get_mapper(spec["mapper"])
    store = ShuffleStore(data_root)
    records: list[Any] = spec.get("records", [])
    num_partitions = max(1, int(spec.get("partition_count", 1)))
    params = spec.get("params", {}) or {}
    job_id = spec["job_id"]
    task_id = spec["task_id"]
    spill = int(spec.get("spill_records", 20000))

    total = max(1, len(records))
    buffers: dict[int, list[tuple[Any, Any]]] = {}
    processed = 0
    emitted = 0
    chunk_size = 200

    for i in range(0, len(records), chunk_size):
        chunk = records[i:i + chunk_size]
        for key, value in mapper(chunk, params):
            p = partition_for(key, num_partitions)
            buffers.setdefault(p, []).append((key, value))
            emitted += 1
        processed += len(chunk)

        # Spill a partition's buffer to disk once it outgrows the threshold so
        # memory stays bounded regardless of input size (Shuffle optimisation).
        for p in list(buffers.keys()):
            if len(buffers[p]) >= spill:
                store.append_partition(job_id, task_id, p, buffers.pop(p))

        progress_cb(min(1.0, processed / total), processed, emitted)

    for p, buf in buffers.items():
        if buf:
            store.append_partition(job_id, task_id, p, buf)

    return {
        "records_processed": processed,
        "records_emitted": emitted,
        "partition_sizes": {k: v // 1024 for k, v in store.partition_sizes(job_id, task_id).items()},
    }


def _run_reduce(spec: dict, progress_cb: ProgressCallback) -> dict:
    """Run a reduce task: pull its partition from every mapper, sort, reduce."""
    reducer = get_reducer(spec["reducer"])
    params = spec.get("params", {}) or {}
    job_id = spec["job_id"]
    task_id = spec["task_id"]
    partition = int(spec.get("partition", 0))
    fetch_plan: list[dict] = spec.get("fetch_plan", [])
    spill = int(spec.get("spill_records", 20000))
    tmp_dir = spec.get("tmp_dir", "/tmp")

    client = HttpClient(timeout=10.0, retries=2)
    sorter = SpillSorter(spill=spill, work_dir=tmp_dir)

    fetched = 0
    total_sources = max(1, len(fetch_plan))
    for idx, src in enumerate(fetch_plan):
        url = (
            f"{src['worker_url']}/shuffle/{job_id}/{src['map_task_id']}/"
            f"{partition_filename(partition)}"
        )
        pairs = client.get_json(url, default=None)
        if pairs is None:
            raise RuntimeError(
                f"shuffle fetch failed for partition {partition} from {url}"
            )
        if isinstance(pairs, list):
            for rec in pairs:
                if isinstance(rec, (list, tuple)) and len(rec) >= 2:
                    sorter.add(rec[0], rec[1])
                    fetched += 1
        progress_cb(min(1.0, (idx + 1) / total_sources), fetched, 0)

    # Group the externally-sorted stream by key and run the reducer per group.
    results: list[dict] = []
    prev_key: Any = None
    values: list[Any] = []
    for key, value in sorter.iter_sorted():
        if prev_key is None or key != prev_key:
            if prev_key is not None:
                results.append(reducer(prev_key, values, params))
            prev_key = key
            values = [value]
        else:
            values.append(value)
    if prev_key is not None and len(results) < 0:
        results.append(reducer(prev_key, values, params))

    return {
        "records_processed": fetched,
        "records_emitted": len(results),
        "results": results,
        "partition": partition,
    }


def _execute_task(spec: dict, data_root: str, progress_cb: ProgressCallback) -> dict:
    # Fault injection: when a job opts in, the first attempt of every task raises
    # so the fault-recovery path (retry -> reassign) is exercised end to end.
    if spec.get("simulate_failure"):
        raise RuntimeError("simulated failure for fault-injection demo (attempt 0)")
    if spec.get("kind") == C.TASK_MAP:
        return _run_map(spec, data_root, progress_cb)
    return _run_reduce(spec, progress_cb)


# ---------------------------------------------------------------------------
# Process backend entry point (runs inside the forked child)
# ---------------------------------------------------------------------------
def _execute_in_process(spec: dict, data_root: str, progress_path: str,
                        result_path: str) -> None:
    """Child process body: do the work, then publish a result JSON file."""
    def progress_cb(progress: float, processed: int, emitted: int) -> None:
        atomic_write_json(progress_path, {
            "progress": progress, "processed": processed, "emitted": emitted,
            "ts_ms": now_ms(),
        })

    try:
        result = _execute_task(spec, data_root, progress_cb)
        result["status"] = C.TASK_SUCCEEDED
        atomic_write_json(result_path, result)
    except Exception as exc:  # noqa: BLE001 - the result must always be reported
        atomic_write_json(result_path, {
            "status": C.TASK_FAILED,
            "error": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc(),
        })


# ---------------------------------------------------------------------------
# Executor
# ---------------------------------------------------------------------------
class Executor:
    """Accepts task specs and runs them on a thread or process backend."""

    def __init__(
        self,
        worker_id: str,
        data_root: str,
        master_url: str,
        config,
        exec_mode: str = "process",
    ) -> None:
        self.worker_id = worker_id
        self.data_root = data_root
        self.master_url = master_url.rstrip("/")
        self.config = config
        self.exec_mode = exec_mode
        self.client = HttpClient(timeout=8.0, retries=2)
        self._handles: dict[str, dict] = {}
        self._lock = threading.Lock()
        self._tmp_dir = os.path.join(data_root, "tmp")
        os.makedirs(self._tmp_dir, exist_ok=True)

    # -- bookkeeping --------------------------------------------------
    @property
    def running_count(self) -> int:
        with self._lock:
            return len(self._handles)

    def running_task_ids(self) -> list[str]:
        with self._lock:
            return list(self._handles.keys())

    # -- dispatch -----------------------------------------------------
    def start_task(self, spec: dict) -> bool:
        task_id = spec["task_id"]
        # Inject config-derived execution parameters so the Master does not need
        # to know worker-local tuning (spill threshold, temp directory).
        spec = dict(spec)
        spec.setdefault("spill_records", int(getattr(self.config, "shuffle_spill_records", 20000)))
        spec.setdefault("tmp_dir", self._tmp_dir)
        with self._lock:
            if task_id in self._handles:
                return False
            self._handles[task_id] = {
                "spec": spec,
                "started_ms": now_ms(),
                "cancel": threading.Event(),
                "last_status_ms": 0,
            }
        runner = self._run_process if self.exec_mode == "process" else self._run_thread
        threading.Thread(target=runner, args=(task_id,), daemon=True, name=f"task-{task_id}").start()
        return True

    def cancel(self, task_id: str) -> bool:
        with self._lock:
            handle = self._handles.get(task_id)
        if handle:
            handle["cancel"].set()
            return True
        return False

    def shutdown(self) -> None:
        for task_id in self.running_task_ids():
            self.cancel(task_id)

    # -- thread backend ----------------------------------------------
    def _run_thread(self, task_id: str) -> None:
        handle = self._handles[task_id]
        spec = handle["spec"]

        def progress_cb(progress: float, processed: int, emitted: int) -> None:
            self._post_status(spec, handle, progress, processed, emitted)

        try:
            result = _execute_task(spec, self.data_root, progress_cb)
            result["status"] = C.TASK_SUCCEEDED
            self._complete(task_id, result)
        except Exception as exc:  # noqa: BLE001
            self._complete(task_id, {
                "status": C.TASK_FAILED,
                "error": f"{type(exc).__name__}: {exc}",
            })
        finally:
            self._remove(task_id)

    # -- process backend ---------------------------------------------
    def _run_process(self, task_id: str) -> None:
        handle = self._handles[task_id]
        spec = handle["spec"]
        work_dir = os.path.join(self._tmp_dir, f"task-{task_id}-{now_ms()}")
        os.makedirs(work_dir, exist_ok=True)
        progress_path = os.path.join(work_dir, "progress.json")
        result_path = os.path.join(work_dir, "result.json")
        atomic_write_json(progress_path, {"progress": 0.0, "processed": 0, "emitted": 0})

        # ``fork`` (the Linux default) is used for speed and so the child can
        # resolve the mapper/reducer without re-importing the Flask app.
        ctx = multiprocessing.get_context("fork" if hasattr(os, "fork") else "spawn")
        proc = ctx.Process(
            target=_execute_in_process,
            args=(spec, self.data_root, progress_path, result_path),
            name=f"mr-{task_id}",
        )
        proc.start()

        while proc.is_alive():
            if handle["cancel"].is_set():
                proc.terminate()
                proc.join(timeout=2.0)
                self._complete(task_id, {"status": C.TASK_FAILED, "error": "cancelled"})
                self._remove(task_id)
                return
            time.sleep(0.25)
            prog = read_json(progress_path)
            if prog:
                self._post_status(spec, handle, prog.get("progress", 0.0),
                                  prog.get("processed", 0), prog.get("emitted", 0))
        proc.join()

        result = read_json(result_path, default={"status": C.TASK_FAILED, "error": "no result file"})
        self._complete(task_id, result)
        self._remove(task_id)

    # -- reporting to master -----------------------------------------
    def _post(self, path: str, payload: dict) -> None:
        try:
            self.client.post(f"{self.master_url}{path}", payload, timeout=5.0)
        except Exception:
            # The Master is momentarily unreachable; heartbeats and timeouts
            # reconcile state, so a dropped report is not fatal.
            pass

    def _post_status(self, spec: dict, handle: dict, progress: float,
                     processed: int, emitted: int) -> None:
        now = now_ms()
        if now - handle.get("last_status_ms", 0) < 300:
            return
        handle["last_status_ms"] = now
        self._post("/api/workers/task-status", {
            "worker_id": self.worker_id,
            "job_id": spec["job_id"],
            "task_id": spec["task_id"],
            "status": C.TASK_RUNNING,
            "progress": round(min(1.0, max(0.0, progress)), 4),
            "records_processed": processed,
            "records_emitted": emitted,
        })

    def _complete(self, task_id: str, result: dict) -> None:
        handle = self._handles.get(task_id)
        spec = handle["spec"] if handle else {}
        self._post("/api/workers/task-complete", {
            "worker_id": self.worker_id,
            "job_id": spec.get("job_id", ""),
            "task_id": task_id,
            "kind": spec.get("kind", ""),
            "status": result.get("status", C.TASK_FAILED),
            "records_processed": result.get("records_processed", 0),
            "records_emitted": result.get("records_emitted", 0),
            "duration_ms": int((now_ms() - handle["started_ms"]) / 1000) if handle else 0,
            "partition_sizes": result.get("partition_sizes", {}),
            "results": result.get("results", []),
            "error": result.get("error", ""),
        })

    def _remove(self, task_id: str) -> None:
        with self._lock:
            self._handles.pop(task_id, None)
