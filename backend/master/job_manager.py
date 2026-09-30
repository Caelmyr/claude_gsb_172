"""Job and task data layer with lifecycle transitions.

The JobManager is the Master's in-memory index over the JSON store: it loads
jobs/tasks on boot, keeps them warm, and persists every mutation atomically.
It also owns the job lifecycle state machine
``PENDING -> MAP -> SHUFFLE -> REDUCE -> SUCCEEDED / FAILED / CANCELLED``.
Task-level transitions (retry, reassignment) are performed through the same
``update_task`` helper so the scheduler and fault-tolerance code share one
serialisation path.
"""

from __future__ import annotations

import os
import threading
from typing import Callable, Optional

from backend.common import constants as C
from backend.common.jsonutil import now_ms
from backend.common.logbus import LogBus
from backend.common.models import Job, Task, new_job
from backend.common.storage import Storage, list_files, list_subdirs, read_json
from backend.master.shard_planner import ShardPlanner
from backend.tasks.registry import has_mapper, has_reducer
from backend.tasks.samples import input_kind_for


class JobManager:
    def __init__(self, storage: Storage, config, logbus: LogBus) -> None:
        self.storage = storage
        self.config = config
        self.logbus = logbus
        self.planner = ShardPlanner(storage, config)
        self._jobs: dict[str, Job] = {}
        self._tasks: dict[str, dict[str, Task]] = {}
        self._lock = threading.RLock()
        self._load()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def _job_path(self, job_id: str) -> list[str]:
        return ["jobs", job_id, "job.json"]

    def _task_path(self, job_id: str, task_id: str) -> list[str]:
        return ["jobs", job_id, "tasks", f"{task_id}.json"]

    def _load(self) -> None:
        jobs_root = self.storage.path("jobs")
        for job_dir in list_subdirs(jobs_root):
            job_id = os.path.basename(job_dir)
            doc = self.storage.read("jobs", job_id, "job.json")
            if doc:
                self._jobs[job_id] = Job.from_dict(doc)
            tasks: dict[str, Task] = {}
            for path in list_files(os.path.join(job_dir, "tasks"), suffix=".json"):
                tdoc = read_json(path)
                if tdoc:
                    task = Task.from_dict(tdoc)
                    tasks[task.task_id] = task
            self._tasks[job_id] = tasks

    def save_job(self, job: Job) -> None:
        self.storage.write(job.to_dict(), *self._job_path(job.job_id))

    def save_task(self, job_id: str, task: Task) -> None:
        self.storage.write(task.to_dict(), *self._task_path(job_id, task.task_id))

    # ------------------------------------------------------------------
    # Submission
    # ------------------------------------------------------------------
    def submit(self, payload: dict) -> Job:
        name = str(payload.get("name") or "untitled").strip() or "untitled"
        mapper = str(payload.get("mapper") or "")
        reducer = str(payload.get("reducer") or "")
        if not has_mapper(mapper):
            raise ValueError(f"unknown mapper: {mapper!r}")
        if not has_reducer(reducer):
            raise ValueError(f"unknown reducer: {reducer!r}")

        defaults = payload.get("_defaults") or {}
        num_map = int(payload.get("num_map_tasks", defaults.get("num_map_tasks", 8)))
        num_reduce = int(payload.get("num_reduce_tasks", defaults.get("num_reduce_tasks", 4)))
        input_rows = int(payload.get("input_rows", defaults.get("input_rows", 12000)))
        params = dict(payload.get("params") or {})
        params["input_kind"] = input_kind_for(mapper)

        job = new_job(name, mapper, reducer, num_map, num_reduce, input_rows, params)

        with self._lock:
            plan = self.planner.plan(job)
            job.num_map_tasks = len(plan["map_tasks"])
            job.map_task_ids = [t.task_id for t in plan["map_tasks"]]
            job.reduce_task_ids = [t.task_id for t in plan["reduce_tasks"]]
            job.status = C.JOB_MAP
            job.started_ms = now_ms()
            job.stats["total_records"] = plan["total_records"] + 1
            job.stats["input_kind"] = params["input_kind"]

            self._jobs[job.job_id] = job
            self._tasks[job.job_id] = {}
            for task in plan["map_tasks"] + plan["reduce_tasks"]:
                self._tasks[job.job_id][task.task_id] = task
                self.save_task(job.job_id, task)
            self.save_job(job)

        self.logbus.info(
            job.job_id, f"job submitted: {job.num_map_tasks} map / {job.num_reduce_tasks} reduce, "
                        f"{plan['total_records']} records", task_id="submit",
        )
        return job

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------
    def get_job(self, job_id: str) -> Optional[Job]:
        with self._lock:
            return self._jobs.get(job_id)

    def list_jobs(self) -> list[Job]:
        with self._lock:
            return sorted(self._jobs.values(), key=lambda j: j.created_ms, reverse=True)

    def get_task(self, job_id: str, task_id: str) -> Optional[Task]:
        with self._lock:
            return self._tasks.get(job_id, {}).get(task_id)

    def tasks_for(self, job_id: str, kind: str = "") -> list[Task]:
        with self._lock:
            tasks = list(self._tasks.get(job_id, {}).values())
        if kind:
            tasks = [t for t in tasks if t.kind == kind]
        return sorted(tasks, key=lambda t: (t.kind, t.index))

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------
    def set_job_status(self, job: Job, status: str) -> Job:
        with self._lock:
            job.status = status
            if status == C.JOB_MAP and not job.started_ms:
                job.started_ms = now_ms()
            if status in C.JOB_TERMINAL_STATES and not job.finished_ms:
                job.finished_ms = now_ms()
            self.save_job(job)
        return job

    def update_task(self, job_id: str, task_id: str, **fields) -> Optional[Task]:
        with self._lock:
            task = self._tasks.get(job_id, {}).get(task_id)
            if task is None:
                return None
            for key, value in fields.items():
                if hasattr(task, key):
                    setattr(task, key, value)
            task.last_update_ms = now_ms()
            self.save_task(job_id, task)
            return task

    def update_job(self, job: Job, **fields) -> Job:
        with self._lock:
            for key, value in fields.items():
                if hasattr(job, key):
                    setattr(job, key, value)
            self.save_job(job)
        return job

    def apply_task(self, job_id: str, task_id: str, fn: Callable[[Task], None]) -> Optional[Task]:
        """Run ``fn(task)`` under the manager lock and persist the result."""
        with self._lock:
            task = self._tasks.get(job_id, {}).get(task_id)
            if task is None:
                return None
            fn(task)
            task.last_update_ms = now_ms()
            self.save_task(job_id, task)
            return task

    def apply_job(self, job_id: str, fn: Callable[[Job], None]) -> Optional[Job]:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            fn(job)
            self.save_job(job)
            return job

    def mark_task_dispatched(self, job: Job, task: Task, worker_id: str) -> None:
        self.update_task(job.job_id, task.task_id,
                         status=C.TASK_ASSIGNED, worker_id=worker_id,
                         assigned_ms=now_ms(), attempts=task.attempts + 0)

    def cancel(self, job: Job) -> Job:
        self.set_job_status(job, C.JOB_CANCELLED)
        self.logbus.warn(job.job_id, "job cancelled", task_id="job")
        return job

    def fail(self, job: Job, error: str) -> Job:
        job.error = error
        self.set_job_status(job, C.JOB_FAILED)
        self.logbus.error(job.job_id, f"job failed: {error}", task_id="job")
        return job

    # ------------------------------------------------------------------
    # Derived views
    # ------------------------------------------------------------------
    def stage_progress(self, job: Job) -> dict:
        tasks = self.tasks_for(job.job_id)
        progress: dict = {}
        for stage, kind in ((C.STAGE_MAP, C.TASK_MAP), (C.STAGE_REDUCE, C.TASK_REDUCE)):
            stage_tasks = [t for t in tasks if t.kind == kind]
            counts = {C.TASK_PENDING: 0, C.TASK_ASSIGNED: 0, C.TASK_RUNNING: 0,
                      C.TASK_RETRYING: 0, C.TASK_SUCCEEDED: 0, C.TASK_FAILED: 0}
            for t in stage_tasks:
                counts[t.status] = counts.get(t.status, 0) + 1
            total = len(stage_tasks)
            done = counts[C.TASK_SUCCEEDED] + counts[C.TASK_RUNNING]
            progress[stage] = {
                "total": total,
                "done": done,
                "pct": round(done / total * 100.0, 1) if total else 0.0,
                "counts": counts,
            }
        return progress

    def job_summary(self, job: Job) -> dict:
        tasks = self.tasks_for(job.job_id)
        by_status: dict[str, int] = {}
        for t in tasks:
            by_status[t.status] = by_status.get(t.status, 0) + 1
        return {
            "job_id": job.job_id,
            "name": job.name,
            "mapper": job.mapper,
            "reducer": job.reducer,
            "status": job.status,
            "num_map_tasks": job.num_map_tasks,
            "num_reduce_tasks": job.num_reduce_tasks,
            "input_rows": job.input_rows,
            "created_ms": job.created_ms,
            "started_ms": job.started_ms,
            "finished_ms": job.finished_ms,
            "error": job.error,
            "params": job.params,
            "stats": job.stats,
            "task_status": by_status,
            "stage_progress": self.stage_progress(job),
        }
