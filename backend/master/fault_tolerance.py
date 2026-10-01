"""Fault tolerance: task retries, worker-death reassignment, speculation.

This module turns failures into *recoverable events*:

* an attempt that runs past ``task_timeout_sec`` is atomically claimed, killed
  on its worker, and then retried or marked failed; duplicate executions of the
  same task are tracked independently, so one timeout cannot affect another job;
* a failed task is retried up to ``max_attempts`` with exponential backoff, and
  only then marks the job failed;
* a worker that stops heartbeating has every in-flight task reassigned to other
  workers (the original is treated as a lost attempt, not a permanent failure);
* stragglers — tasks running much longer than the median — are detected and a
  speculative duplicate is launched on another worker, winner takes all.

Every decision is recorded both as a structured ``FaultEvent`` document (for the
fault-recovery page) and as a log line (for the log-search page).
"""

from __future__ import annotations

import statistics
from typing import Optional

from backend.common import constants as C
from backend.common.ids import new_id
from backend.common.jsonutil import now_ms
from backend.common.logbus import LogBus
from backend.common.models import FaultEvent, Job, Task, WorkerRecord
from backend.common.storage import Storage
from backend.master.job_manager import JobManager


EXECUTIONS_KEY = "executions"
EXECUTION_ACTIVE_STATES = {C.TASK_ASSIGNED, C.TASK_RUNNING}
TIMED_OUT = "TIMED_OUT"
LOST = "LOST"


def _executions(task: Task) -> list[dict]:
    value = (task.stats or {}).get(EXECUTIONS_KEY, [])
    return [dict(e) for e in value if isinstance(e, dict)]


def _find_execution(task: Task, execution_id: str) -> tuple[int, Optional[dict]]:
    for i, execution in enumerate(_executions(task)):
        if execution.get("execution_id") == execution_id:
            return i, execution
    return -1, None


def _active_executions(task: Task, *, exclude_id: str = "") -> list[dict]:
    return [
        e for e in _executions(task)
        if e.get("status") in EXECUTION_ACTIVE_STATES and e.get("execution_id") != exclude_id
    ]


def _set_executions(task: Task, executions: list[dict]) -> None:
    stats = dict(task.stats or {})
    stats[EXECUTIONS_KEY] = executions
    task.stats = stats


def _mark_execution(executions: list[dict], index: int, **fields) -> dict:
    execution = dict(executions[index])
    execution.update(fields)
    executions[index] = execution
    return execution


def _promote_execution(task: Task, execution: dict, timed_execution_id: str) -> None:
    task.execution_id = execution["execution_id"]
    task.worker_id = execution.get("worker_id")
    task.status = C.TASK_RUNNING if execution.get("status") == C.TASK_RUNNING else C.TASK_ASSIGNED
    if execution.get("started_ms"):
        task.started_ms = int(execution["started_ms"])

    stats = dict(task.stats or {})
    stats["speculated"] = False
    executions = _executions(task)
    updated = []
    for entry in executions:
        entry = dict(entry)
        if entry.get("execution_id") == timed_execution_id:
            entry["status"] = TIMED_OUT
            entry["finished_ms"] = now_ms()
        if entry.get("execution_id") == execution["execution_id"]:
            entry["speculative"] = False
            entry["promoted_ms"] = now_ms()
        updated.append(entry)
    stats[EXECUTIONS_KEY] = updated
    task.stats = stats


class FaultTolerance:
    def __init__(
        self,
        storage: Storage,
        job_manager: JobManager,
        config,
        logbus: LogBus,
    ) -> None:
        self.storage = storage
        self.job_manager = job_manager
        self.config = config
        self.logbus = logbus

    # ------------------------------------------------------------------
    def _record(self, job: Job, kind: str, message: str, task: Optional[Task] = None,
                worker_id: str = "", detail: Optional[dict] = None) -> FaultEvent:
        event = FaultEvent(
            fault_id=new_id("fault"),
            job_id=job.job_id,
            task_id=task.task_id if task else "",
            worker_id=worker_id,
            kind=kind,
            message=message,
            attempt=task.attempts if task else 0,
            created_ms=now_ms(),
            detail=detail or {},
        )
        self.storage.write(event.to_dict(), "jobs", job.job_id, "faults", f"{event.fault_id}.json")
        self.logbus.warn(
            job.job_id, f"[{kind}] {message}",
            task_id=task.task_id if task else "job", worker_id=worker_id,
        )
        return event

    # ------------------------------------------------------------------
    def claim_task_timeout(self, job: Job, task: Task, execution_id: str) -> Optional[dict]:
        """Atomically claim one expired execution.

        Returns one of:
        * ``{"kind": "speculation", "execution": ...}`` — only a duplicate timed out;
        * ``{"kind": "promoted", "execution": ...}`` — a live duplicate takes over;
        * ``{"kind": "retrying", "execution": ...}`` — the canonical attempt is being retried;
        * ``{"kind": "failed", "execution": ...}`` — retry budget is exhausted.
        A completion that wins the race makes this return ``None``.
        """
        result: dict[str, Optional[dict]] = {"outcome": None}

        def transition(t: Task) -> None:
            executions = _executions(t)
            index, execution = _find_execution(t, execution_id)

            # Orphaned state from an older master / a dispatch acknowledgement
            # that was lost has no execution ledger. Treat the task itself as
            # the canonical expired attempt so it cannot wedge the job.
            orphan = (
                not executions and not t.execution_id and not execution_id
                and t.status in EXECUTION_ACTIVE_STATES
            )
            if orphan:
                index = -1
                execution = {
                    "execution_id": "",
                    "worker_id": t.worker_id or "",
                    "assigned_ms": t.assigned_ms,
                }

            if index < 0 and not orphan:
                return
            if not orphan and execution.get("status") not in EXECUTION_ACTIVE_STATES:
                return

            now = now_ms()
            timeout_sec = float(self.config.task_timeout_sec)
            elapsed_ms = now - int(execution.get("assigned_ms") or t.assigned_ms or now)
            error = f"timed out after {timeout_sec:g}s"
            is_canonical = execution_id == t.execution_id

            if not is_canonical and not orphan:
                execution = _mark_execution(
                    executions, index, status=TIMED_OUT, finished_ms=now, error=error,
                )
                _set_executions(t, executions)
                result["outcome"] = {"kind": "speculation", "execution": execution}
                return

            survivors = [
                e for e in executions
                if e.get("execution_id") != execution_id
                and e.get("status") in EXECUTION_ACTIVE_STATES
            ]
            if index >= 0:
                execution = _mark_execution(
                    executions, index, status=TIMED_OUT, finished_ms=now, error=error,
                )
            if survivors:
                _set_executions(t, executions)
                winner = survivors[0]
                _promote_execution(t, winner, execution_id)
                result["outcome"] = {"kind": "promoted", "execution": winner,
                                     "timed_out": execution}
                return

            max_attempts = int(self.config.max_attempts)
            next_attempt = t.attempts + 1
            will_retry = next_attempt < max_attempts
            _set_executions(t, executions)

            stats = dict(t.stats or {})
            stats.pop(EXECUTIONS_KEY, None)
            stats.pop("speculated", None)
            t.stats = stats
            t.attempts = next_attempt
            t.worker_id = None
            t.execution_id = ""
            t.error = error
            t.progress = 0.0
            t.records_processed = 0
            t.records_emitted = 0
            t.started_ms = 0
            t.assigned_ms = 0
            t.finished_ms = 0
            t.retry_after_ms = 0

            if will_retry:
                t.status = C.TASK_RETRYING
                t.retry_after_ms = now + int(
                    self.config.retry_backoff_base_sec * (2 ** (next_attempt - 1))
                )
                kind = "retrying"
            else:
                t.status = C.TASK_FAILED
                t.finished_ms = now
                kind = "failed"
            result["outcome"] = {"kind": kind, "execution": execution,
                                 "elapsed_ms": elapsed_ms}

        updated = self.job_manager.apply_task(job.job_id, task.task_id, transition)
        outcome = result["outcome"]
        if updated is not None and outcome is not None:
            for key, value in updated.__dict__.items():
                setattr(task, key, value)
        return outcome

    def handle_task_failure(
        self,
        job: Job,
        task: Task,
        error: str,
        worker_id: str = "",
        execution_id: str = "",
    ) -> bool:
        """Return True if the task was queued for retry, False if the job is doomed."""
        max_attempts = int(self.config.max_attempts)
        will_retry = task.attempts < max_attempts - 1

        def transition(t: Task) -> bool:
            orphan = (
                not execution_id and not t.execution_id
                and not _executions(t) and t.status in EXECUTION_ACTIVE_STATES
            )
            if not orphan and execution_id and t.execution_id != execution_id:
                index, current = _find_execution(t, execution_id)
                if index < 0 or current.get("status") not in EXECUTION_ACTIVE_STATES:
                    return False
                executions = _executions(t)
                _mark_execution(
                    executions, index,
                    status=C.TASK_FAILED, worker_id=worker_id, error=error,
                    finished_ms=now_ms(),
                )
                _set_executions(t, executions)
                return False

            next_attempt = t.attempts + 1
            t.attempts = next_attempt
            t.worker_id = None
            t.execution_id = ""
            t.error = error
            t.progress = 0.0
            t.records_processed = 0
            t.records_emitted = 0
            t.started_ms = 0
            t.assigned_ms = 0
            t.finished_ms = 0

            stats = dict(t.stats or {})
            stats.pop(EXECUTIONS_KEY, None)
            stats.pop("speculated", None)
            t.stats = stats

            if will_retry:
                t.status = C.TASK_RETRYING
                t.retry_after_ms = now_ms() + int(
                    self.config.retry_backoff_base_sec * (2 ** (next_attempt - 1))
                )
            else:
                t.status = C.TASK_FAILED
                t.retry_after_ms = 0
                t.finished_ms = now_ms()
            return True

        canonical: Optional[Task] = self.job_manager.apply_task(job.job_id, task.task_id, transition)
        if canonical is None or canonical.status not in (C.TASK_RETRYING, C.TASK_FAILED):
            return False

        if will_retry:
            backoff_ms = max(0, canonical.retry_after_ms - now_ms())
            self._record(
                job, "task_failed", f"task {canonical.task_id} failed ({error}); retrying",
                task=canonical, worker_id=worker_id,
                detail={"attempt": canonical.attempts, "max_attempts": max_attempts,
                        "backoff_ms": backoff_ms, "execution_id": execution_id},
            )
            return True

        self._record(
            job, "task_failed", f"task {canonical.task_id} exhausted {max_attempts} attempts",
            task=canonical, worker_id=worker_id,
        )
        self.job_manager.fail(job, f"task {canonical.task_id} failed after {max_attempts} attempts: {error}")
        return False

    def handle_worker_death(self, worker: WorkerRecord) -> int:
        """Reassign every in-flight task on a dead worker. Returns count."""
        reassigned = 0
        for job in self.job_manager.list_jobs():
            if job.is_terminal:
                continue
            for task in self.job_manager.tasks_for(job.job_id):
                affected = self._claim_worker_death(job, task, worker.worker_id)
                if affected:
                    self._record(
                        job, "worker_dead",
                        f"worker {worker.name} lost; reassigning task {task.task_id}",
                        task=task, worker_id=worker.worker_id,
                    )
                    reassigned += 1
        return reassigned

    def _claim_worker_death(self, job: Job, task: Task, worker_id: str) -> str:
        result = {"kind": ""}

        def transition(t: Task) -> None:
            executions = _executions(t)
            canonical_dead = (
                t.worker_id == worker_id
                and t.execution_id
                and t.status in EXECUTION_ACTIVE_STATES
            )
            spec_dead: list[int] = []
            for index, execution in enumerate(executions):
                if (
                    execution.get("worker_id") == worker_id
                    and execution.get("execution_id") != t.execution_id
                    and execution.get("status") in EXECUTION_ACTIVE_STATES
                ):
                    spec_dead.append(index)

            if not canonical_dead and not spec_dead:
                return

            now = now_ms()
            for index in spec_dead:
                _mark_execution(
                    executions, index, status=LOST, finished_ms=now,
                    error="worker died",
                )

            if not canonical_dead:
                _set_executions(t, executions)
                result["kind"] = "speculation"
                return

            survivors = [
                e for e in executions
                if e.get("status") in EXECUTION_ACTIVE_STATES and e.get("worker_id") != worker_id
            ]
            if survivors:
                _set_executions(t, executions)
                _promote_execution(t, survivors[0], t.execution_id)
                result["kind"] = "promoted"
                return

            def reset_for_retry(current: Task) -> None:
                stats = dict(current.stats or {})
                stats.pop(EXECUTIONS_KEY, None)
                stats.pop("speculated", None)
                current.stats = stats
                current.status = C.TASK_RETRYING
                current.worker_id = None
                current.execution_id = ""
                current.error = "worker died"
                current.retry_after_ms = 0
                current.progress = 0.0
                current.records_processed = 0
                current.records_emitted = 0
                current.started_ms = 0
                current.assigned_ms = 0
                current.finished_ms = 0

            _set_executions(t, executions)
            reset_for_retry(t)
            result["kind"] = "reassigned"

        updated = self.job_manager.apply_task(job.job_id, task.task_id, transition)
        if updated is not None and result["kind"]:
            # Reload the caller's dataclass so log detail and later stages see it.
            for key, value in updated.__dict__.items():
                setattr(task, key, value)
        return result["kind"]

    def find_stragglers(self, job: Job) -> list[Task]:
        """Tasks running far longer than the median, still awaiting a duplicate."""
        if not self.config.speculative_execution:
            return []
        tasks = self.job_manager.tasks_for(job.job_id)
        running = [t for t in tasks if t.status == C.TASK_RUNNING]
        finished = [t for t in tasks if t.status == C.TASK_SUCCEEDED and t.duration_ms > 0]
        if not running or len(finished) < 2:
            return []
        median = statistics.median(t.duration_ms for t in finished)
        threshold = median * float(self.config.speculation_threshold)
        stragglers: list[Task] = []
        for t in running:
            elapsed = now_ms() - (t.started_ms or now_ms())
            if elapsed > threshold and not (t.stats or {}).get("speculated"):
                stragglers.append(t)
        return stragglers

    def mark_speculated(self, job: Job, task: Task) -> None:
        stats = dict(task.stats or {})
        stats["speculated"] = True
        self.job_manager.update_task(job.job_id, task.task_id, stats=stats)

    def executions(self, task: Task) -> list[dict]:
        return _executions(task)

    def find_execution(self, task: Task, execution_id: str) -> tuple[int, Optional[dict]]:
        return _find_execution(task, execution_id)

    def list_faults(self, job_id: str) -> list[dict]:
        from backend.common.storage import list_files, read_json
        root = self.storage.path("jobs", job_id, "faults")
        faults = []
        for path in list_files(root, suffix=".json"):
            doc = read_json(path)
            if doc:
                faults.append(doc)
        faults.sort(key=lambda d: d.get("created_ms", 0))
        return faults
