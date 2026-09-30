"""Fault tolerance: task retries, worker-death reassignment, speculation.

This module turns failures into *recoverable events*:

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
    def handle_task_failure(self, job: Job, task: Task, error: str, worker_id: str = "") -> bool:
        """Return True if the task was queued for retry, False if the job is doomed."""
        max_attempts = int(self.config.max_attempts)
        if task.attempts < max_attempts:
            backoff_ms = int(self.config.retry_backoff_base_sec * (2 ** task.attempts))
            self._record(
                job, "task_failed", f"task {task.task_id} failed ({error}); retrying",
                task=task, worker_id=worker_id,
                detail={"attempt": task.attempts + 1, "max_attempts": max_attempts,
                        "backoff_ms": backoff_ms},
            )
            self.job_manager.update_task(
                job.job_id, task.task_id,
                status=C.TASK_RETRYING, worker_id=None, error=error,
                attempts=task.attempts + 1,
                retry_after_ms=now_ms() + backoff_ms,
                progress=0.0, records_processed=0, records_emitted=0,
            )
            return True

        self._record(
            job, "task_failed", f"task {task.task_id} exhausted {max_attempts} attempts",
            task=task, worker_id=worker_id,
        )
        self.job_manager.update_task(job.job_id, task.task_id, status=C.TASK_FAILED,
                                     error=error, attempts=task.attempts + 1)
        self.job_manager.fail(job, f"task {task.task_id} failed after {max_attempts} attempts: {error}")
        return False

    def handle_worker_death(self, worker: WorkerRecord) -> int:
        """Reassign every in-flight task on a dead worker. Returns count."""
        reassigned = 0
        for job in self.job_manager.list_jobs():
            if job.is_terminal:
                continue
            for task in self.job_manager.tasks_for(job.job_id):
                if task.worker_id == worker.worker_id and task.status in C.TASK_ACTIVE_STATES:
                    self._record(
                        job, "worker_dead",
                        f"worker {worker.name} lost; reassigning task {task.task_id}",
                        task=task, worker_id=worker.worker_id,
                    )
                    self.job_manager.update_task(
                        job.job_id, task.task_id,
                        status=C.TASK_RETRYING, worker_id=None,
                        error=f"worker {worker.name} died", retry_after_ms=0,
                    )
                    reassigned += 1
        return reassigned

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
