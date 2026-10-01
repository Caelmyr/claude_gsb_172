"""The Master scheduler: dispatch, stage advancement and progress handling.

A single background thread runs a ``tick`` loop that:

1. reaps workers whose heartbeat timed out (delegating reassignment to
   ``FaultTolerance``);
2. enforces the per-attempt ``task_timeout_sec`` deadline, retrying or failing
   the task instead of allowing a stuck job to remain active forever;
3. for each active job, dispatches pending map/reduce tasks to the least-loaded
   alive worker and advances the stage state machine;
4. checks for stragglers and launches speculative duplicates.

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
from backend.common.ids import new_id, partition_name
from backend.common.jsonutil import now_ms
from backend.common.logbus import LogBus
from backend.common.models import Job, Task, WorkerRecord
from backend.common.storage import Storage
from backend.master import fault_tolerance as ft_module
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
            wait = max(0.05, float(self.config.scheduler_tick_sec))
            self._stop.wait(wait)

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

        # 2. Enforce task deadlines before dispatching so stuck attempts never
        #    leave an active job waiting forever.
        for job in self.job_manager.list_jobs():
            if job.is_terminal:
                continue
            try:
                self._enforce_task_timeouts(job)
            except Exception:  # noqa: BLE001
                traceback.print_exc()

        # 3. Advance each active job.
        for job in self.job_manager.list_jobs():
            if job.is_terminal:
                continue
            try:
                self._advance(job)
            except Exception:  # noqa: BLE001
                traceback.print_exc()

    # ------------------------------------------------------------------
    def _enforce_task_timeouts(self, job: Job) -> None:
        timeout_ms = int(float(self.config.task_timeout_sec) * 1000)
        now = now_ms()
        for task in self.job_manager.tasks_for(job.job_id):
            if task.status not in (C.TASK_ASSIGNED, C.TASK_RUNNING):
                continue
            expired = []
            canonical_expired = False
            executions = self.fault_tolerance.executions(task)
            if not executions and not task.execution_id and task.assigned_ms:
                if now - task.assigned_ms >= timeout_ms:
                    expired.append({
                        "execution_id": "",
                        "worker_id": task.worker_id or "",
                        "assigned_ms": task.assigned_ms,
                    })
                    canonical_expired = True

            for execution in executions:
                if execution.get("status") not in ft_module.EXECUTION_ACTIVE_STATES:
                    continue
                assigned_ms = int(execution.get("assigned_ms") or task.assigned_ms or 0)
                if assigned_ms and now - assigned_ms >= timeout_ms:
                    expired.append(execution)
                    if execution.get("execution_id") == task.execution_id:
                        canonical_expired = True

            # Process the canonical attempt first; if a live duplicate takes
            # over, scheduler ticks under the new deadline govern it.
            expired.sort(key=lambda e: e.get("execution_id") != task.execution_id)
            for execution in expired:
                execution_id = execution.get("execution_id", "")
                current = self.job_manager.get_task(job.job_id, task.task_id)
                if current is None or current.status not in (C.TASK_ASSIGNED, C.TASK_RUNNING):
                    break
                if not canonical_expired and execution_id == current.execution_id:
                    continue
                outcome = self.fault_tolerance.claim_task_timeout(job, current, execution_id)
                if outcome is None:
                    continue
                self._handle_timeout_outcome(job, current, execution_id, outcome)
                if outcome["kind"] in ("retrying", "failed"):
                    break

    def _handle_timeout_outcome(
        self, job: Job, task: Task, execution_id: str, outcome: dict,
    ) -> None:
        execution = outcome.get("execution") or {}
        worker_id = str(execution.get("worker_id") or task.worker_id or "")
        kind = outcome["kind"]
        timeout_sec = float(self.config.task_timeout_sec)
        timeout_ms = int(timeout_sec * 1000)

        if kind == "speculation":
            self._cancel_execution(job, task, execution_id, reason="speculative attempt timeout")
            self.logbus.warn(
                job.job_id,
                f"speculative copy of {task.task_id} timed out; canonical attempt continues",
                task_id=task.task_id, worker_id=worker_id,
            )
            return

        self._cancel_execution(job, task, execution_id, reason="task timeout")
        if kind == "promoted":
            self.fault_tolerance._record(
                job, "task_timeout",
                f"task {task.task_id} timed out after {timeout_sec:g}s; live speculative copy took over",
                task=task, worker_id=worker_id,
                detail={"execution_id": execution_id, "timeout_sec": timeout_sec},
            )
            return

        self.fault_tolerance._record(
            job, "task_timeout",
            f"task {task.task_id} timed out after {timeout_sec:g}s",
            task=task, worker_id=worker_id,
            detail={
                "execution_id": execution_id,
                "timeout_sec": timeout_sec,
                "elapsed_ms": outcome.get("elapsed_ms", timeout_ms),
                "retrying": kind == "retrying",
            },
        )
        if kind == "failed":
            self.job_manager.fail(
                job,
                f"task {task.task_id} timed out after {int(self.config.max_attempts)} attempts: "
                f"timed out after {timeout_sec:g}s",
            )

    def _cancel_execution(self, job: Job, task: Task, execution_id: str, reason: str = "") -> None:
        _, execution = self.fault_tolerance.find_execution(task, execution_id)
        worker_id = str((execution or {}).get("worker_id") or (task.worker_id if not execution_id else "") or "")
        worker = self.registry.get(worker_id)
        if worker is None:
            return
        try:
            self.client.post(f"{worker.address}/task/cancel", {
                "job_id": job.job_id,
                "task_id": task.task_id,
                "execution_id": execution_id,
                "reason": reason,
            }, timeout=2.0)
        except Exception:  # noqa: BLE001 - the master timeout state remains authoritative
            pass

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
                for execution in self.fault_tolerance.executions(task):
                    if (
                        execution.get("worker_id") == worker_id
                        and execution.get("status") in ft_module.EXECUTION_ACTIVE_STATES
                        and execution.get("execution_id") != task.execution_id
                    ):
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
        execution_id = new_id("exec")
        assigned_ms = now_ms()
        spec = self._build_spec(job, task, execution_id, assigned_ms, speculative=speculative)
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

        execution = {
            "execution_id": execution_id,
            "worker_id": worker.worker_id,
            "worker_name": worker.name,
            "status": C.TASK_ASSIGNED,
            "speculative": bool(speculative),
            "assigned_ms": assigned_ms,
            "started_ms": 0,
            "finished_ms": 0,
            "attempt": task.attempts,
        }

        def mark_dispatched(t: Task) -> None:
            stats = dict(t.stats or {})
            executions = self.fault_tolerance.executions(t)
            if speculative:
                executions.append(execution)
                stats["speculated"] = True
            else:
                # Canonical dispatch: stale records belong only to an earlier attempt.
                executions = [execution]
                stats.pop("speculated", None)
                t.status = C.TASK_ASSIGNED
                t.worker_id = worker.worker_id
                t.execution_id = execution_id
                t.assigned_ms = assigned_ms
                t.started_ms = 0
                t.finished_ms = 0
                t.retry_after_ms = 0
                t.error = ""
                t.progress = 0.0
                t.records_processed = 0
                t.records_emitted = 0
            stats[ft_module.EXECUTIONS_KEY] = executions
            t.stats = stats

        self.job_manager.apply_task(job.job_id, task.task_id, mark_dispatched)
        self.logbus.info(
            job.job_id,
            f"task {task.task_id} dispatched to {worker.name}" + (" (speculative)" if speculative else ""),
            task_id=task.task_id, worker_id=worker.worker_id,
        )

    def _build_spec(self, job: Job, task: Task, execution_id: str, assigned_ms: int,
                    speculative: bool = False) -> dict:
        timeout_sec = max(0.05, float(self.config.task_timeout_sec))
        spec: dict = {
            "task_id": task.task_id,
            "job_id": job.job_id,
            "execution_id": execution_id,
            "kind": task.kind,
            "index": task.index,
            "mapper": job.mapper,
            "reducer": job.reducer,
            "params": job.params,
            "attempt": task.attempts,
            "timeout_sec": timeout_sec,
            "deadline_ms": assigned_ms + int(timeout_sec * 1000),
            "speculative": bool(speculative),
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

        execution_id = str(payload.get("execution_id") or "")
        worker_id = str(payload.get("worker_id") or "")
        orphan_execution_id = new_id("exec-legacy")

        def apply(t: Task) -> bool:
            nonlocal execution_id
            stats = dict(t.stats or {})
            executions = self.fault_tolerance.executions(t)
            index, execution = self.fault_tolerance.find_execution(t, execution_id)
            if index < 0:
                # Compatibility with a worker that accepted a task before this
                # Master version recorded execution ledgers. Re-adopt it exactly
                # once using the task's existing assigned_ms deadline.
                if execution_id or t.execution_id or executions or t.status not in ft_module.EXECUTION_ACTIVE_STATES:
                    return False
                execution_id = orphan_execution_id
                assigned_ms = int(t.assigned_ms or now_ms())
                execution = {
                    "execution_id": execution_id,
                    "worker_id": t.worker_id or worker_id,
                    "status": t.status,
                    "assigned_ms": assigned_ms,
                    "started_ms": t.started_ms or assigned_ms,
                    "legacy": True,
                }
                executions = [execution]
                index = 0
                t.execution_id = execution_id
                if worker_id and not t.worker_id:
                    t.worker_id = worker_id
            elif execution.get("status") not in ft_module.EXECUTION_ACTIVE_STATES:
                return False

            now = now_ms()
            started_ms = int(execution.get("started_ms") or now)
            execution = dict(execution)
            execution["status"] = C.TASK_RUNNING
            execution["started_ms"] = started_ms
            execution["last_update_ms"] = now
            executions[index] = execution

            is_canonical = execution_id == t.execution_id
            if is_canonical:
                t.status = C.TASK_RUNNING
                if not t.started_ms:
                    t.started_ms = started_ms
                t.progress = float(payload.get("progress", t.progress))
                t.records_processed = int(payload.get("records_processed", t.records_processed))
                t.records_emitted = int(payload.get("records_emitted", t.records_emitted))

            stats = dict(t.stats or {})
            stats[ft_module.EXECUTIONS_KEY] = executions
            t.stats = stats
            return True

        self.job_manager.apply_task(job.job_id, task.task_id, apply)

    def on_task_complete(self, payload: dict) -> None:
        job = self.job_manager.get_job(payload.get("job_id", ""))
        if job is None:
            return
        task = self.job_manager.get_task(job.job_id, payload.get("task_id", ""))
        if task is None or task.status in C.TASK_TERMINAL_STATES:
            return  # duplicate completion from an old attempt or completed task

        execution_id = str(payload.get("execution_id") or "")
        worker_id = str(payload.get("worker_id", ""))
        status = payload.get("status", C.TASK_FAILED)
        duration_ms = max(0, int(payload.get("duration_ms", 0)))

        if status != C.TASK_SUCCEEDED:
            self.fault_tolerance.handle_task_failure(
                job, task, payload.get("error", ""), worker_id, execution_id,
            )
            self.registry.task_finished(worker_id, success=False)
            return

        completed = {"task": None}

        def apply_success(t: Task) -> None:
            nonlocal execution_id
            index, execution = self.fault_tolerance.find_execution(t, execution_id)
            now = now_ms()
            executions = self.fault_tolerance.executions(t)
            if index < 0:
                if execution_id or t.execution_id or executions or t.status not in ft_module.EXECUTION_ACTIVE_STATES:
                    return
                execution_id = new_id("exec-legacy")
                assigned_ms = int(t.assigned_ms or now)
                execution = {
                    "execution_id": execution_id,
                    "worker_id": t.worker_id or worker_id,
                    "status": t.status,
                    "assigned_ms": assigned_ms,
                    "started_ms": t.started_ms or assigned_ms,
                    "legacy": True,
                }
                executions = [execution]
                index = 0
                t.execution_id = execution_id
                if worker_id and not t.worker_id:
                    t.worker_id = worker_id
            elif execution.get("status") not in ft_module.EXECUTION_ACTIVE_STATES:
                return
            for i, candidate in enumerate(executions):
                candidate = dict(candidate)
                if i == index:
                    candidate.update({
                        "status": C.TASK_SUCCEEDED,
                        "finished_ms": now,
                        "records_processed": int(payload.get("records_processed", 0)),
                        "records_emitted": int(payload.get("records_emitted", 0)),
                        "duration_ms": duration_ms,
                    })
                elif candidate.get("status") in ft_module.EXECUTION_ACTIVE_STATES:
                    candidate["status"] = ft_module.LOST
                    candidate["finished_ms"] = now
                    candidate["error"] = "another speculative copy completed first"
                executions[i] = candidate

            t.status = C.TASK_SUCCEEDED
            t.worker_id = worker_id
            t.execution_id = execution_id
            t.progress = 1.0
            t.records_processed = int(payload.get("records_processed", 0))
            t.records_emitted = int(payload.get("records_emitted", 0))
            t.duration_ms = duration_ms
            t.finished_ms = now
            t.error = ""
            if not t.started_ms and execution.get("started_ms"):
                t.started_ms = int(execution["started_ms"])
            stats = dict(t.stats or {})
            stats[ft_module.EXECUTIONS_KEY] = executions
            stats["partition_size_entries"] = payload.get("partition_sizes", {})
            stats["results"] = payload.get("results", [])
            stats["winning_worker"] = worker_id
            t.stats = stats
            completed["task"] = t

        self.job_manager.apply_task(job.job_id, task.task_id, apply_success)
        winner = completed["task"]
        if winner is None:
            return

        self.registry.task_finished(worker_id, success=True)
        self.metrics.record_task(job, winner, duration_ms)

        if winner.kind == C.TASK_REDUCE:
            self._store_results(job, winner, payload.get("results", []))
            self.shuffle.mark_partition_done(job, winner.partition,
                                             winner.stats.get("shuffle_bytes", 0))

        self.logbus.info(
            job.job_id,
            f"task {winner.task_id} succeeded ({payload.get('records_processed', 0)} records, "
            f"{duration_ms} ms)",
            task_id=winner.task_id, worker_id=worker_id,
        )
        self._cancel_speculative_losers(job, winner)

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

    def _cancel_speculative_losers(self, job: Job, task: Task) -> None:
        for execution in self.fault_tolerance.executions(task):
            execution_id = execution.get("execution_id", "")
            wid = execution.get("worker_id", "")
            if execution_id == task.execution_id or not execution_id or not wid:
                continue
            worker = self.registry.get(wid)
            if worker is None:
                continue
            try:
                self.client.post(f"{worker.address}/task/cancel", {
                    "job_id": job.job_id,
                    "task_id": task.task_id,
                    "execution_id": execution_id,
                    "reason": "another speculative copy completed first",
                }, timeout=2.0)
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
            self._dispatch(job, task, worker, speculative=True)
            self.logbus.warn(job.job_id, f"speculative copy of {task.task_id} -> {worker.name}",
                             task_id=task.task_id)
