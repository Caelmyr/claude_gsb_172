"""Typed records exchanged between the Master, Workers and the JSON store.

Every record is a ``@dataclass`` that round-trips through JSON via
``to_dict`` / ``from_dict``.  Timestamps are epoch-milliseconds integers so
there is no datetime serialization ambiguity.  ``_version`` is the optimistic
concurrency field maintained by ``storage``.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Optional

from . import constants as C
from .ids import new_id
from .jsonutil import now_ms


# ---------------------------------------------------------------------------
# Worker registry record (Master's authoritative view of a node)
# ---------------------------------------------------------------------------
@dataclass
class WorkerRecord:
    worker_id: str
    name: str = ""
    host: str = "127.0.0.1"
    port: int = 0
    status: str = C.WORKER_ALIVE
    cpu_cores: int = 0
    mem_total_mb: int = 0
    registered_ms: int = 0
    last_heartbeat_ms: int = 0
    cpu_percent: float = 0.0
    mem_percent: float = 0.0
    load1: float = 0.0
    running_tasks: int = 0
    queued_tasks: int = 0
    total_tasks_completed: int = 0
    total_tasks_failed: int = 0
    exec_mode: str = "process"
    version: int = 0

    @property
    def address(self) -> str:
        return f"http://{self.host}:{self.port}"

    @property
    def load_score(self) -> float:
        """Composite load used by the scheduler; lower is better."""
        return (
            self.running_tasks * 1.0
            + self.queued_tasks * 0.5
            + self.load1 * 0.25
            + self.cpu_percent * 0.01
        )

    @property
    def is_alive(self) -> bool:
        return self.status == C.WORKER_ALIVE

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "WorkerRecord":
        known = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in d.items() if k in known})


# ---------------------------------------------------------------------------
# Task record
# ---------------------------------------------------------------------------
@dataclass
class Task:
    task_id: str
    job_id: str
    kind: str  # C.TASK_MAP | C.TASK_REDUCE
    index: int = 0
    status: str = C.TASK_PENDING
    worker_id: Optional[str] = None
    attempts: int = 0
    input_shard: str = ""            # for map tasks: in-XXXX
    partition_count: int = 0          # for map: R, for reduce: 1
    partition: int = 0                # for reduce tasks: its partition index
    created_ms: int = 0
    assigned_ms: int = 0
    started_ms: int = 0
    finished_ms: int = 0
    last_update_ms: int = 0
    progress: float = 0.0
    records_processed: int = 0
    records_emitted: int = 0
    duration_ms: int = 0
    retry_after_ms: int = 0
    error: str = ""
    stats: dict = field(default_factory=dict)
    version: int = 0

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Task":
        known = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in d.items() if k in known})


# ---------------------------------------------------------------------------
# Job record
# ---------------------------------------------------------------------------
@dataclass
class Job:
    job_id: str
    name: str
    mapper: str
    reducer: str
    num_map_tasks: int
    num_reduce_tasks: int
    input_rows: int = 0
    status: str = C.JOB_PENDING
    created_ms: int = 0
    started_ms: int = 0
    finished_ms: int = 0
    map_task_ids: list[str] = field(default_factory=list)
    reduce_task_ids: list[str] = field(default_factory=list)
    stage_progress: dict = field(default_factory=dict)   # stage -> {done, total, pct}
    stats: dict = field(default_factory=dict)
    error: str = ""
    params: dict = field(default_factory=dict)
    version: int = 0

    @property
    def is_terminal(self) -> bool:
        return self.status in C.JOB_TERMINAL_STATES

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Job":
        known = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in d.items() if k in known})


# ---------------------------------------------------------------------------
# Shard record (one per input shard / map task / reduce task / shuffle partition)
# ---------------------------------------------------------------------------
@dataclass
class Shard:
    shard_id: str
    job_id: str
    stage: str
    index: int = 0
    status: str = C.TASK_PENDING
    worker_id: Optional[str] = None
    size: int = 0
    records: int = 0
    created_ms: int = 0
    updated_ms: int = 0
    data: Any = None
    version: int = 0

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Shard":
        known = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in d.items() if k in known})


# ---------------------------------------------------------------------------
# Fault / retry event
# ---------------------------------------------------------------------------
@dataclass
class FaultEvent:
    fault_id: str
    job_id: str
    task_id: str = ""
    worker_id: str = ""
    kind: str = "task_failed"        # task_failed | worker_dead | reassigned | speculation
    message: str = ""
    attempt: int = 0
    created_ms: int = 0
    detail: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "FaultEvent":
        known = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in d.items() if k in known})


# ---------------------------------------------------------------------------
# Metric sample (appended to a JSONL metrics file)
# ---------------------------------------------------------------------------
@dataclass
class MetricSample:
    ts_ms: int
    job_id: str = ""
    worker_id: str = ""
    records_per_sec: float = 0.0
    task_latency_ms: float = 0.0
    cpu_percent: float = 0.0
    mem_percent: float = 0.0
    load1: float = 0.0
    tasks_running: int = 0
    tasks_completed: int = 0
    throughput: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "MetricSample":
        known = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in d.items() if k in known})


# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------
def new_job(name: str, mapper: str, reducer: str, num_map_tasks: int,
            num_reduce_tasks: int, input_rows: int, params: Optional[dict] = None) -> Job:
    return Job(
        job_id=new_id("job"),
        name=name,
        mapper=mapper,
        reducer=reducer,
        num_map_tasks=num_map_tasks,
        num_reduce_tasks=num_reduce_tasks,
        input_rows=input_rows,
        created_ms=now_ms(),
        params=params or {},
        stage_progress={s: {"done": 0, "total": 0, "pct": 0.0} for s in C.STAGES},
    )


def new_task(job: Job, kind: str, index: int) -> Task:
    t = Task(
        task_id=f"{kind[0]}-{index:04d}",
        job_id=job.job_id,
        kind=kind,
        index=index,
        created_ms=now_ms(),
    )
    if kind == C.TASK_MAP:
        t.input_shard = f"in-{index:04d}"
        t.partition_count = job.num_reduce_tasks
    else:
        t.partition = index
        t.partition_count = 1
    return t


def new_worker(worker_id: str, name: str, host: str, port: int, cpu_cores: int,
               mem_total_mb: int, exec_mode: str = "process") -> WorkerRecord:
    return WorkerRecord(
        worker_id=worker_id,
        name=name,
        host=host,
        port=port,
        cpu_cores=cpu_cores,
        mem_total_mb=mem_total_mb,
        registered_ms=now_ms(),
        last_heartbeat_ms=now_ms(),
        exec_mode=exec_mode,
    )
