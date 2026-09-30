"""Shuffle coordination: partition matrix, fetch plans and progress.

When the last map task completes, the Master builds the shuffle plan:

* for every reduce partition, a JSON record listing every mapper that produced
  that partition and how many bytes it emitted — this is the matrix the
  shuffle-view page renders;
* a *fetch plan* attached to each reduce task, telling the reducer which mapper
  URL to pull the partition from.

This coordinator is what turns "N maps each produced R partitions" into "R
reducers each pulling N files", and it is the single place the Shuffle phase is
modelled, so progress and transfer sizes are all derived from it.
"""

from __future__ import annotations

from typing import Optional

from backend.common import constants as C
from backend.common.ids import partition_name
from backend.common.jsonutil import now_ms
from backend.common.logbus import LogBus
from backend.common.models import Job
from backend.common.storage import Storage, list_files, read_json
from backend.master.job_manager import JobManager
from backend.master.registry import WorkerRegistry


class ShuffleCoordinator:
    def __init__(
        self,
        storage: Storage,
        job_manager: JobManager,
        registry: WorkerRegistry,
        logbus: LogBus,
    ) -> None:
        self.storage = storage
        self.job_manager = job_manager
        self.registry = registry
        self.logbus = logbus

    # ------------------------------------------------------------------
    def _partition_doc_path(self, job_id: str, partition: int) -> list[str]:
        return ["jobs", job_id, "shuffle", f"{partition_name(partition)}.json"]

    def build(self, job: Job) -> None:
        """Build the shuffle matrix and reduce fetch plans once all maps are done."""
        map_tasks = self.job_manager.tasks_for(job.job_id, C.TASK_MAP)
        reduce_tasks = self.job_manager.tasks_for(job.job_id, C.TASK_REDUCE)

        for rt in reduce_tasks:
            p = rt.partition
            pname = partition_name(p)
            sources: list[dict] = []
            total_bytes = 0
            for mt in map_tasks:
                worker = self.registry.get(mt.worker_id or "")
                if worker is None:
                    continue
                sizes = (mt.stats or {}).get("partition_sizes", {}) or {}
                byte_count = int(sizes.get(f"{pname}.jsonl", 0))
                sources.append({
                    "map_task_id": mt.task_id,
                    "worker_id": mt.worker_id,
                    "worker_url": worker.address,
                    "bytes": byte_count,
                })
                total_bytes += byte_count + 1

            self.storage.write({
                "job_id": job.job_id,
                "partition": p,
                "partition_name": pname,
                "sources": sources,
                "num_sources": len(sources),
                "total_bytes": total_bytes,
                "reduce_task_id": rt.task_id,
                "status": "ready",
                "fetched_bytes": 0,
                "built_ms": now_ms(),
                "updated_ms": now_ms(),
            }, *self._partition_doc_path(job.job_id, p))

            # Attach the fetch plan to the reduce task.
            fetch_plan = [
                {"worker_url": s["worker_url"], "map_task_id": s["map_task_id"]}
                for s in sources
            ]
            rt.stats["fetch_plan"] = fetch_plan
            rt.stats["num_sources"] = len(fetch_plan)
            rt.stats["shuffle_bytes"] = total_bytes
            self.job_manager.save_task(job.job_id, rt)

        self.logbus.info(
            job.job_id,
            f"shuffle plan built: {len(map_tasks)} maps x {len(reduce_tasks)} partitions",
            task_id="shuffle",
        )

    def mark_partition_done(self, job: Job, partition: int, fetched_bytes: int = 0) -> None:
        self.storage.update(
            lambda doc: {
                **(doc or {}),
                "status": "done",
                "fetched_bytes": fetched_bytes,
                "updated_ms": now_ms(),
            },
            *self._partition_doc_path(job.job_id, partition),
        )

    def matrix(self, job: Job) -> dict:
        """Full partition matrix plus per-partition and aggregate summaries."""
        partitions: list[dict] = []
        for path in list_files(self.storage.path("jobs", job.job_id, "shuffle"), suffix=".json"):
            doc = read_json(path)
            if doc:
                partitions.append(doc)
        partitions.sort(key=lambda d: d.get("partition", 0))

        total_bytes = sum(d.get("total_bytes", 0) for d in partitions)
        done = sum(1 for d in partitions if d.get("status") in ("done", "ready"))
        return {
            "job_id": job.job_id,
            "num_partitions": len(partitions),
            "partitions_done": done,
            "total_bytes": total_bytes,
            "progress_pct": round(done / len(partitions) * 100.0, 1) if partitions else 0.0,
            "partitions": partitions,
        }

    def progress(self, job: Job) -> dict:
        m = self.matrix(job)
        return {
            "done": m["partitions_done"],
            "total": m["num_partitions"],
            "pct": m["progress_pct"],
            "total_bytes": m["total_bytes"],
        }
