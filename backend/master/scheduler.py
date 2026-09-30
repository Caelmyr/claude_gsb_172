"""The Master scheduler: dispatch, stage advancement and progress handling.

A single background thread runs a ``tick`` loop that:

1. reaps workers whose heartbeat timed out (delegating reassignment to
   ``FaultTolerance``);
2. for each active job, dispatches pending map/reduce tasks to the least-loaded
   alive worker and advances the stage state machine;
3. checks for stragglers and launches speculative duplicates.

Task *completions* arrive asynchronously over HTTP (from workers) and are
handled by ``on_task_complete`` / ``on_task_status``, which mutate state through
the JobManager's locked helpers so the Flask threads and the scheduler thread
never race.
"""

from __future__ import annotations

import threading
import traceback
from typing import Optional

from backend.common import constants as C
from backend.common.http_client import HttpClient
from backend.common.ids import partition_name
from backend.common.jsonutil import now_ms
from backend.common.logbus import LogBus
from backend.common.models import Job, Task, WorkerRecord
from backend.common.storage import Storage
from backend.master.fault_tolerance import FaultTolerance
from backend.master.job_manager import JobManager
from backend.master.metrics import Metrics
from backend.master.registry import WorkerRegistry
from backend.master.shuffle import ShuffleCoordinator

SHUFFLE_HOLD_MS = 400          # keep the SHUFFLE stage observable for one beat
MAX_TASKS_PER_WORKER = 3       # concurrency cap per worker


class Scheduler:
    def __init__(
        self,
        storage: Storage,
        job_manager: JobManager,
        registry: WorkerRegistry,
        shuffle: ShuffleCoordinator,
        fault_tolerance: FaultTolerance,
        metrics: Metrics,
        config,
        logbus: LogBus,
    ) -> None:
        self.storage = storage
        self.job_manager = job_manager
        self.registry = registry
        self.shuffle = shuffle
        self.fault_tolerance = fault_tolerance
        self.metrics = metrics
        self.config = config
        self.logbus = logbus
        self.client = HttpClient(timeout=3.0, retries=1)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="scheduler")

    # ------------------------------------------------------------------
    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception:  # noqa: BLE001 - a scheduler crash must not kill the Master
                traceback.print_exc()
            self._stop.wait(self.config.metric_interval_sec)

    # ------------------------------------------------------------------
    def tick(self) -> None:
        # 1. Reap dead workers and reassign their tasks (on a coarser cadence).
        self._tick_count = getattr(self, "_tick_count", 0) + 1
        if self._tick_count % 5 == 0:
            for worker in self.registry.reap():
                count = self.fault_tolerance.handle_worker_death(worker)
                if count:
                    self.logbus.warn("", f"worker {worker.name} reaped; {count} tasks reassigned",
                                     task_id="cluster")

        # 2. Advance each active job.
        for job in self.job_manager.list_jobs():
            if job.is_terminal:
                continue
            try:
                self._advance(job)
            except Exception:  # noqa: BLE001
                traceback.print_exc()

    # ------------------------------------------------------------------
    def _advance(self, job: Job) -> None:
        status = job.status
        if status == C.JOB_MAP:
            self._dispatch_tasks(job, C.TASK_MAP)
            map_tasks = self.job_manager.tasks_for(job.job_id, C.TASK_MAP)
            if map_tasks and all(t.status == C.TASK_SUCCEEDED for t in map_tasks):
                self.shuffle.build(job)
                self.job_manager.apply_job(job.job_id, lambda j: (
                    setattr(j, "status", C.JOB_SHUFFLE),
                    j.stats.__setitem__("shuffle_started_ms", now_ms()),
                ))
                self.logbus.info(job.job_id, "all map tasks finished; shuffle built",
                                 task_id="shuffle")
        elif status == C.JOB_SHUFFLE:
            started = job.stats.get("shuffle_started_ms", 0)
            if now_ms() - started >= SHUFFLE_HOLD_MS:
                self.job_manager.set_job_status(job, C.JOB_REDUCE)
                self.logbus.info(job.job_id, "shuffle complete; reduce stage started",
                                 task_id="shuffle")
        elif status == C.JOB_REDUCE:
            self._dispatch_tasks(job, C.TASK_REDUCE)
            reduce_tasks = self.job_manager.tasks_for(job.job_id, C.TASK_REDUCE)
            if reduce_tasks and all(t.status == C.TASK_SUCCEEDED for t in reduce_tasks):
                self._finish_success(job)

        self._maybe_speculate(job)

    # ------------------------------------------------------------------
    def _dispatch_tasks(self, job: Job, kind: str) -> None:
        pending = [
            t for t in self.job_manager.tasks_for(job.job_id, kind)
            if t.status in (C.TASK_PENDING, C.TASK_RETRYING)
        ]
        if not pending:
            return
        workers = self._available_workers()
        if not workers:
            return

        for task in pending:
            if task.status == C.TASK_RETRYING and task.retry_after_ms > now_ms():
                continue  # exponential backoff not yet elapsed
            worker = self._least_loaded(workers, exclude=None)
            if worker is None:
                return
            self._dispatch(job, task, worker)

    def _available_workers(self) -> list[WorkerRecord]:
        out: list[WorkerRecord] = []
        for worker in self.registry.alive():
            capacity = max(1, min(worker.cpu_cores or 2, MAX_TASKS_PER_WORKER))
            running = self._count_running_on(worker.worker_id)
            if running < capacity:
                out.append(worker)
        return out

    def _count_running_on(self, worker_id: str) -> int:
        count = 0
        for job in self.job_manager.list_jobs():
            if job.is_terminal:
                continue
            for task in self.job_manager.tasks_for(job.job_id):
                if task.worker_id == worker_id and task.status in C.TASK_ACTIVE_STATES:
                    count += 1
        return count

    def _least_loaded(self, workers: list[WorkerRecord],
                      exclude: Optional[str] = None) -> Optional[WorkerRecord]:
        candidates = [w for w in workers if w.worker_id != exclude] or workers
        if not candidates:
            return None

        def load(w: WorkerRecord) -> float:
            return w.load1 * 2.0 + w.cpu_percent * 0.01

        return min(candidates, key=load)

    # ------------------------------------------------------------------
    def _dispatch(self, job: Job, task: Task, worker: WorkerRecord,
                  speculative: bool = False) -> None:
        spec = self._build_spec(job, task)
        if speculative:
            spec["speculative"] = True
        url = f"{worker.address}/task/execute"
        try:
            resp = self.client.post(url, spec, timeout=4.0)
            accepted = bool(resp.ok and resp.data and resp.data.get("accepted"))
        except Exception as exc:  # noqa: BLE001
            accepted = False
            self.logbus.warn(job.job_id, f"dispatch to {worker.name} failed: {exc}",
                             task_id=task.task_id)
        if not accepted:
            return

        def mark_dispatched(t: Task) -> None:
            t.status = C.TASK_ASSIGNED
            t.assigned_ms = now_ms()
            if not speculative:
                t.worker_id = worker.worker_id
            else:
                stats = dict(t.stats or {})
                stats.setdefault("speculative_workers", []).append(worker.worker_id)
                t.stats = stats

        self.job_manager.apply_task(job.job_id, task.task_id, mark_dispatched)
        self.logbus.info(
            job.job_id,
            f"task {task.task_id} dispatched to {worker.name}" + (" (speculative)" if speculative else ""),
            task_id=task.task_id, worker_id=worker.worker_id,
        )

    def _build_spec(self, job: Job, task: Task) -> dict:
        spec: dict = {
            "task_id": task.task_id,
            "job_id": job.job_id,
            "kind": task.kind,
            "index": task.index,
            "mapper": job.mapper,
            "reducer": job.reducer,
            "params": job.params,
            "attempt": 0,
            "simulate_failure": bool(job.params.get("simulate_failure", False)),
        }
        if task.kind == C.TASK_MAP:
            spec["partition_count"] = job.num_reduce_tasks
            spec["records"] = self.job_manager.planner.load_input_shard(job.job_id, task.input_shard)
        else:
            spec["partition"] = task.partition
            spec["fetch_plan"] = (task.stats or {}).get("fetch_plan", [])
            spec["num_map_tasks"] = job.num_map_tasks
        return spec

    # ------------------------------------------------------------------
    # Completion / progress handling (invoked from Flask routes)
    # ------------------------------------------------------------------
    def on_task_status(self, payload: dict) -> None:
        job = self.job_manager.get_job(payload.get("job_id", ""))
        if job is None:
            return
        task = self.job_manager.get_task(job.job_id, payload.get("task_id", ""))
        if task is None or task.status == C.TASK_SUCCEEDED:
            return

        def apply(t: Task) -> None:
            if t.status in (C.TASK_PENDING, C.TASK_RETRYING, C.TASK_ASSIGNED):
                t.status = C.TASK_RUNNING
                t.worker_id = payload.get("worker_id", t.worker_id)
            if not t.started_ms:
                t.started_ms = now_ms()
            t.progress = float(payload.get("progress", t.progress))
            t.records_processed = int(payload.get("records_processed", t.records_processed))
            t.records_emitted = int(payload.get("records_emitted", t.records_emitted))

        self.job_manager.apply_task(job.job_id, task.task_id, apply)

    def on_task_complete(self, payload: dict) -> None:
        job = self.job_manager.get_job(payload.get("job_id", ""))
        if job is None:
            return
        task = self.job_manager.get_task(job.job_id, payload.get("task_id", ""))
        if task is None or task.status == C.TASK_SUCCEEDED:
            return  # duplicate completion from a speculative loser

        worker_id = payload.get("worker_id", "")
        status = payload.get("status", C.TASK_FAILED)

        if status != C.TASK_SUCCEEDED:
            self.registry.task_finished(worker_id, success=False)
            self.fault_tolerance.handle_task_failure(job, task, payload.get("error", ""), worker_id)
            return

        # Success path.
        def apply(t: Task) -> None:
            t.status = C.TASK_SUCCEEDED
            t.progress = 1.0
            t.records_processed = int(payload.get("records_processed", 0))
            t.records_emitted = int(payload.get("records_emitted", 0))
            t.duration_ms = int(payload.get("duration_ms", 0)) * 1000
            t.finished_ms = now_ms()
            t.error = ""
            stats = dict(t.stats or {})
            stats["partition_size_entries"] = payload.get("partition_sizes", {})
            stats["results"] = payload.get("results", [])
            stats["winning_worker"] = worker_id
            t.stats = stats

        self.job_manager.apply_task(job.job_id, task.task_id, apply)
        self.registry.task_finished(worker_id, success=True)
        self.metrics.record_task(job, task, int(payload.get("duration_ms", 0)))

        if task.kind == C.TASK_REDUCE:
            self._store_results(job, task, payload.get("results", []))
            self.shuffle.mark_partition_done(job, task.partition,
                                             task.stats.get("shuffle_bytes", 0))

        self.logbus.info(
            job.job_id,
            f"task {task.task_id} succeeded ({payload.get('records_processed', 0)} records, "
            f"{payload.get('duration_ms', 0)} ms)",
            task_id=task.task_id, worker_id=worker_id,
        )
        self._cancel_speculative_losers(job, task, worker_id)

    def _store_results(self, job: Job, task: Task, results: list) -> None:
        pname = partition_name(task.partition)
        self.storage.write({
            "job_id": job.job_id,
            "partition": task.partition,
            "partition_name": pname,
            "task_id": task.task_id,
            "records": list(reversed(results)),
            "count": len(results),
            "written_ms": now_ms(),
        }, "jobs", job.job_id, "results", C.STAGE_REDUCE, f"{pname}.json")

    def _cancel_speculative_losers(self, job: Job, task: Task, winner_worker_id: str) -> None:
        losers = list((task.stats or {}).get("speculative_workers", []))
        if winner_worker_id != task.worker_id and task.worker_id:
            losers.append(task.worker_id)
        for wid in losers:
            if wid == winner_worker_id:
                continue
            worker = self.registry.get(wid)
            if worker is not None:
                try:
                    self.client.post(f"{worker.address}/task/cancel",
                                     {"task_id": task.task_id}, timeout=2.0)
                except Exception:  # noqa: BLE001
                    pass

    # ------------------------------------------------------------------
    def _finish_success(self, job: Job) -> None:
        map_tasks = self.job_manager.tasks_for(job.job_id, C.TASK_MAP)
        reduce_tasks = self.job_manager.tasks_for(job.job_id, C.TASK_REDUCE)

        def apply(j: Job) -> None:
            j.status = C.JOB_SUCCEEDED
            j.finished_ms = now_ms()
            j.stats["map_records_processed"] = sum(t.records_processed for t in map_tasks)
            j.stats["map_records_emitted"] = sum(t.records_emitted for t in map_tasks)
            j.stats["reduce_records_emitted"] = sum(t.records_emitted for t in reduce_tasks) + sum(t.records_emitted for t in map_tasks)
            j.stats["total_task_attempts"] = sum(t.attempts for t in map_tasks + reduce_tasks)

        self.job_manager.apply_job(job.job_id, apply)
        self.logbus.info(job.job_id, "job succeeded", task_id="job")
        self._cleanup_worker_shuffle(job)

    def _cleanup_worker_shuffle(self, job: Job) -> None:
        for worker in self.registry.alive():
            try:
                self.client.post(f"{worker.address}/shuffle/cleanup/{job.job_id}", timeout=2.0)
            except Exception:  # noqa: BLE001
                pass

    # ------------------------------------------------------------------
    def _maybe_speculate(self, job: Job) -> None:
        for task in self.fault_tolerance.find_stragglers(job):
            workers = self._available_workers()
            worker = self._least_loaded(workers, exclude=task.worker_id)
            if worker is None:
                continue
            self.fault_tolerance.mark_speculated(job, task)
            self._dispatch(job, task, worker, speculative=True)
            self.logbus.warn(job.job_id, f"speculative copy of {task.task_id} -> {worker.name}",
                             task_id=task.task_id)
