"""Structured JSON logging shared by Master and Worker.

Logs are the observability backbone of a MapReduce run.  Each record is one
JSON line appended to ``jobs/{job_id}/logs/{stage}/{task_id}.jsonl``; because
records are appended atomically (via ``storage.append_jsonl``) many workers and
the master can emit concurrently without corruption.  ``query`` streams every
file under a job's log directory and filters, so the log-search page has a
single, simple backend.
"""

from __future__ import annotations

import os
from typing import Any, Optional

from . import constants as C
from .jsonutil import now_ms
from .storage import Storage, list_files, read_jsonl_stream


class LogBus:
    """Emit and query structured log records stored as JSONL shards."""

    def __init__(self, storage: Storage) -> None:
        self.storage = storage

    # -- emit ---------------------------------------------------------
    def emit(
        self,
        job_id: str,
        level: str,
        message: str,
        stage: str = "master",
        task_id: str = "job",
        worker_id: str = "",
        **extra: Any,
    ) -> dict:
        record: dict[str, Any] = {
            "ts": now_ms(),
            "level": level,
            "stage": stage,
            "task_id": task_id,
            "worker_id": worker_id,
            "message": message,
        }
        record.update(extra)
        # Stage directory keeps the log shards grouped exactly as the frontend
        # log-search page expects (by stage and task).
        self.storage.append(record, "jobs", job_id, "logs", stage, f"{task_id}.jsonl")
        return record

    def debug(self, job_id: str, message: str, **kw: Any) -> dict:
        return self.emit(job_id, C.LOG_DEBUG, message, **kw)

    def info(self, job_id: str, message: str, **kw: Any) -> dict:
        return self.emit(job_id, C.LOG_INFO, message, **kw)

    def warn(self, job_id: str, message: str, **kw: Any) -> dict:
        return self.emit(job_id, C.LOG_WARN, message, **kw)

    def error(self, job_id: str, message: str, **kw: Any) -> dict:
        return self.emit(job_id, C.LOG_ERROR, message, **kw)

    # -- query --------------------------------------------------------
    def _log_root(self, job_id: str) -> str:
        return self.storage.path("jobs", job_id, "logs")

    def stages(self, job_id: str) -> list[str]:
        root = self._log_root(job_id)
        if not os.path.isdir(root):
            return []
        return sorted(e for e in os.listdir(root) if os.path.isdir(os.path.join(root, e)))

    def query(
        self,
        job_id: str,
        search: str = "",
        stage: str = "",
        task_id: str = "",
        level: str = "",
        limit: int = 500,
    ) -> dict:
        """Return matching log records (newest last) plus a summary count."""
        needle = (search or "").lower()
        records: list[dict] = []
        total_scanned = 0
        for path in list_files(self._log_root(job_id), suffix=".jsonl", recursive=True):
            rel = os.path.relpath(path, self._log_root(job_id))
            parts = rel.split(os.sep)
            f_stage = parts[0] if len(parts) > 1 else "master"
            f_task = os.path.basename(path)[: -len(".jsonl")]
            if stage and f_stage != stage:
                continue
            if task_id and f_task != task_id:
                continue
            for rec in read_jsonl_stream(path):
                total_scanned += 1
                if level and rec.get("level") != level:
                    continue
                if needle:
                    hay = _lower_record(rec)
                    if needle not in hay:
                        continue
                rec.setdefault("stage", f_stage)
                rec.setdefault("task_id", f_task)
                records.append(rec)

        records.sort(key=lambda r: r.get("ts_ms", 0))
        total = len(records)
        return {
            "total": total,
            "scanned": total_scanned,
            "stages": self.stages(job_id),
            "records": records[:limit],
        }


def _lower_record(rec: dict) -> str:
    """Cheap case-insensitive haystack over a record's textual fields."""
    parts = [str(rec.get("message", ""))]
    for key in ("level", "stage", "task_id", "worker_id"):
        parts.append(str(rec.get(key, "")))
    return " ".join(parts).lower()
