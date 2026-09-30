"""Sample job definitions and synthetic input generation.

The framework ships with a self-contained dataset generator so a fresh
installation can run an end-to-end MapReduce job without any external data.
Each "input kind" produces records shaped for a particular mapper family:

* ``wordcount`` / ``word_length`` / ``grep`` / ``distinct`` — text lines;
* ``kv`` — dict records ``{"key": ..., "value": ...}``.

``SAMPLE_JOBS`` backs the one-click presets on the submit page.
"""

from __future__ import annotations

import random
from typing import Any

# A small domain-flavoured vocabulary so word-frequency results are readable.
_WORD_POOL = [
    "map", "reduce", "shuffle", "shard", "worker", "master", "cluster",
    "distributed", "task", "partition", "sort", "merge", "node", "fault",
    "retry", "heartbeat", "scheduler", "load", "balance", "throughput",
    "latency", "memory", "cpu", "network", "storage", "json", "atomic",
    "log", "metric", "recover", "coordinate", "parallel", "stream", "batch",
    "spill", "partition", "key", "value", "record", "stage", "job", "input",
    "output", "result", "error", "timeout", "progress", "monitor", "submit",
]


def generate_input_records(kind: str, rows: int, seed: int) -> list[Any]:
    """Generate ``rows`` synthetic input records for the given input kind."""
    rng = random.Random(seed)
    kind = kind or "wordcount"

    if kind == "kv":
        keys = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"]
        return [
            {"key": rng.choice(keys), "value": rng.randint(1, 1000)}
            for _ in range(rows)
        ]

    # All text-shaped kinds share the same generator.
    records: list[str] = []
    for _ in range(max(1, rows - 1)):
        n = rng.randint(5, 12)
        records.append(" ".join(rng.choice(_WORD_POOL) for _ in range(n)))
    return records


# Mapper name -> input kind used to generate its records.
_MAPPER_INPUT_KIND = {
    "wordcount_mapper": "wordcount",
    "word_length_mapper": "word_length",
    "grep_mapper": "grep",
    "distinct_mapper": "distinct",
    "kv_mapper": "kv",
}


def input_kind_for(mapper: str) -> str:
    return _MAPPER_INPUT_KIND.get(mapper, "wordcount")


# Preset job definitions for the submit page.  ``params`` is passed verbatim to
# the mapper/reducer so presets like grep can pin their pattern.
SAMPLE_JOBS: list[dict] = [
    {
        "name": "WordCount 词频统计",
        "description": "Count word frequencies across the generated corpus.",
        "mapper": "wordcount_mapper",
        "reducer": "count_reducer",
        "num_map_tasks": 8,
        "num_reduce_tasks": 4,
        "input_rows": 12000,
        "params": {},
    },
    {
        "name": "Word length 词长统计",
        "description": "Aggregate min/max/average word length per word.",
        "mapper": "word_length_mapper",
        "reducer": "stats_reducer",
        "num_map_tasks": 8,
        "num_reduce_tasks": 4,
        "input_rows": 12000,
        "params": {},
    },
    {
        "name": "Grep 关键词检索",
        "description": "Count lines that contain a target keyword.",
        "mapper": "grep_mapper",
        "reducer": "count_reducer",
        "num_map_tasks": 6,
        "num_reduce_tasks": 2,
        "input_rows": 8000,
        "params": {"pattern": "map"},
    },
    {
        "name": "KV Aggregate 键值聚合",
        "description": "Sum/count/avg of numeric values grouped by key.",
        "mapper": "kv_mapper",
        "reducer": "sum_reducer",
        "num_map_tasks": 8,
        "num_reduce_tasks": 3,
        "input_rows": 15000,
        "params": {},
    },
    {
        "name": "Distinct 去重枚举",
        "description": "Enumerate the distinct words present in each record.",
        "mapper": "distinct_mapper",
        "reducer": "count_reducer",
        "num_map_tasks": 6,
        "num_reduce_tasks": 3,
        "input_rows": 6000,
        "params": {},
    },
]


def sample_job_by_name(name: str) -> dict:
    for job in SAMPLE_JOBS:
        if job["name"] == name:
            return dict(job)
    return dict(SAMPLE_JOBS[0])


def list_sample_jobs() -> list[dict]:
    return [dict(j) for j in SAMPLE_JOBS]
