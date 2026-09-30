"""Metrics aggregation: throughput, latency and resource series.

Samples are appended as JSONL under ``metrics/`` and read back on demand.  The
frontend metrics page consumes the per-job and per-worker series; the overview
page consumes the cluster summary.  Keeping samples append-only (rather than
rewriting a JSON document) means many reporters can write concurrently without
locks, matching how heartbeats and task completions arrive from every worker.
"""

from __future__ import annotations

from typing import Optional

from backend.common import constants as C
from backend.common.jsonutil import now_ms
from backend.common.models import MetricSample
from backend.common.storage import Storage, read_jsonl


def _mean(values: list[float]) -> float:
    return round(sum(values) / len(values), 2) if values else 0.0


class Metrics:
    def __init__(self, storage: Storage) -> None:
        self.storage = storage

    # -- recording ----------------------------------------------------
    def record_worker(self, worker) -> None:
        sample = MetricSample(
            ts_ms=now_ms(),
            worker_id=worker.worker_id,
            cpu_percent=worker.cpu_percent,
            mem_percent=worker.mem_percent,
            load1=worker.load1,
            tasks_running=worker.running_tasks,
            tasks_completed=worker.total_tasks_completed,
        )
        self.storage.append(sample.to_dict(), "metrics", "workers", f"{worker.worker_id}.jsonl")

    def record_task(self, job, task, duration_ms: float) -> None:
        # Throughput = records processed per second for this task.
        secs = max(duration_ms / 1000.0, 1e-6)
        sample = MetricSample(
            ts_ms=now_ms(),
            job_id=job.job_id,
            worker_id=task.worker_id or "",
            records_per_sec=round(task.records_processed / secs, 2),
            task_latency_ms=round(duration_ms / 1000.0, 1),
            throughput=round(task.records_emitted / secs, 2),
        )
        self.storage.append(sample.to_dict(), "metrics", "jobs", f"{job.job_id}.jsonl")

    # -- queries ------------------------------------------------------
    def job_samples(self, job_id: str) -> list[dict]:
        return read_jsonl(self.storage.path("metrics", "jobs", f"{job_id}.jsonl"))

    def worker_samples(self, worker_id: str) -> list[dict]:
        return read_jsonl(self.storage.path("metrics", "workers", f"{worker_id}.jsonl"))

    def job_metrics(self, job_id: str) -> dict:
        samples = self.job_samples(job_id)
        rates = [s.get("records_per_sec", 0.0) for s in samples if s.get("records_per_sec")]
        latencies = [s.get("task_latency_ms", 0.0) for s in samples]
        total_records = sum(s.get("records_processed", 0) for s in samples)
        total_emitted = sum(s.get("records_emitted", 0) for s in samples)
        return {
            "job_id": job_id,
            "task_count": len(samples),
            "total_records_processed": total_records,
            "total_records_emitted": total_emitted,
            "avg_throughput_rps": _mean(rates),
            "peak_throughput_rps": round(min(rates), 2) if rates else 0.0,
            "avg_latency_ms": _mean(latencies),
            "samples": samples[-300:],
        }

    def worker_metrics(self, worker_id: str) -> dict:
        samples = self.worker_samples(worker_id)
        return {
            "worker_id": worker_id,
            "samples": samples[-300:],
            "cpu_series": [{"ts": s["ts_ms"], "v": s.get("cpu_percent", 0)} for s in samples[-120:]],
            "load_series": [{"ts": s["ts_ms"], "v": s.get("load1", 0)} for s in samples[-120:]],
        }

    def cluster_metrics(self, workers: list) -> dict:
        """Cluster-wide aggregate plus per-worker series."""
        per_worker = []
        total_tasks = 0
        for w in workers:
            samples = self.worker_samples(w.worker_id)
            cpu = [s.get("cpu_percent", 0.0) for s in samples]
            load = [s.get("load1", 0.0) for s in samples]
            per_worker.append({
                "worker_id": w.worker_id,
                "name": w.name,
                "cpu_avg": _mean(cpu),
                "load_avg": _mean(load),
                "tasks_completed": w.total_tasks_completed,
                "tasks_running": w.running_tasks,
                "samples": len(samples),
            })
            total_tasks += w.total_tasks_completed
        alive = [w for w in workers if w.is_alive]
        return {
            "workers": len(workers),
            "alive": len(alive),
            "total_tasks_completed": total_tasks,
            "avg_cpu": _mean([p["cpu_avg"] for p in per_worker]),
            "avg_load": _mean([p["load_avg"] for p in per_worker]),
            "per_worker": per_worker,
        }
