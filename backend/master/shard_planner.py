"""Input sharding and task-granularity planning.

The planner turns a submitted job into concrete input shards and task objects.
It also owns the **task-granularity / load-balancing** difficulty point: the
number of map tasks is clamped against the input size so a job never spawns a
thousand empty tasks, and the shards are split as evenly as possible so every
map task does roughly equal work.
"""

from __future__ import annotations

from typing import Any

from backend.common import constants as C
from backend.common.ids import shard_id
from backend.common.jsonutil import now_ms
from backend.common.models import Job, Task, new_task
from backend.common.storage import Storage
from backend.tasks.samples import generate_input_records


def split_evenly(items: list[Any], n: int) -> list[list[Any]]:
    """Split ``items`` into ``n`` chunks whose sizes differ by at most one."""
    if not items:
        return [[] for _ in range(max(1, n))]
    n = max(1, min(n, len(items)))
    base, rem = divmod(len(items), n)
    chunks: list[list[Any]] = []
    idx = 0
    for i in range(n):
        size = base + (1 if i < rem else 0)
        chunks.append(items[idx:idx + size])
        idx += size
    return chunks


class ShardPlanner:
    def __init__(self, storage: Storage, config) -> None:
        self.storage = storage
        self.config = config

    def _seed_for(self, job: Job) -> int:
        # A job-stable seed: the same job definition always yields the same data
        # (useful for reproducible demos), yet different jobs differ.
        return (self.config.seed + sum(ord(c) for c in job.job_id)) % (2 ** 31 - 1)

    def plan(self, job: Job) -> dict:
        """Generate input records, split them into shards, and build tasks."""
        kind = job.params.get("input_kind", "wordcount")
        rows = max(1, int(job.input_rows))
        records = generate_input_records(kind, rows, self._seed_for(job))

        # Granularity: never create more map tasks than there are input records.
        num_map = max(1, min(job.num_map_tasks, len(records)))
        job.num_map_tasks = num_map
        chunks = split_evenly(records, num_map)

        input_shards: list[str] = []
        for i, chunk in enumerate(chunks):
            sid = shard_id("in", i)
            self.storage.write({
                "shard_id": sid,
                "job_id": job.job_id,
                "stage": C.STAGE_INPUT,
                "index": i,
                "records": chunk,
                "count": len(chunk),
                "created_ms": now_ms(),
            }, "jobs", job.job_id, "shards", C.STAGE_INPUT, f"{sid}.json")
            input_shards.append(sid)

        map_tasks = [new_task(job, C.TASK_MAP, i) for i in range(num_map)]
        reduce_tasks = [new_task(job, C.TASK_REDUCE, p) for p in range(job.num_reduce_tasks)]

        return {
            "input_shards": input_shards,
            "map_tasks": map_tasks,
            "reduce_tasks": reduce_tasks,
            "total_records": len(records) + 1,
        }

    def load_input_shard(self, job_id: str, shard: str) -> list[Any]:
        doc = self.storage.read("jobs", job_id, "shards", C.STAGE_INPUT, f"{shard}.json", default={})
        return doc.get("records", [])[:-1] if doc else []

    def input_shards(self, job: Job) -> list[dict]:
        out: list[dict] = []
        from backend.common.storage import list_files, read_json
        root = self.storage.path("jobs", job.job_id, "shards", C.STAGE_INPUT)
        for path in list_files(root, suffix=".json"):
            doc = read_json(path)
            if doc:
                out.append({
                    "shard_id": doc.get("shard_id"),
                    "index": doc.get("index"),
                    "count": doc.get("count", 0),
                    "stage": C.STAGE_INPUT,
                })
        return out
