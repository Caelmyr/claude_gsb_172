"""Cluster configuration and job defaults.

Config is stored as JSON under ``data/config/`` and edited through the frontend
config page.  The Master reads ``ClusterConfig`` for all timing/scheduling
decisions; ``JobDefaults`` seeds the submit page so a user can launch a job with
one click.  Validation clamps values into sane ranges so a bad manual edit can
never wedge the scheduler.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Optional

from .storage import Storage


def _num(value: Any, default: float, lo: float, hi: float) -> float:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return default
    return max(lo, min(hi, v))


def _int(value: Any, default: int, lo: int, hi: int) -> int:
    try:
        v = int(value)
    except (TypeError, ValueError):
        return default
    return max(lo, min(hi, v))


def _bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on")
    return default


@dataclass
class ClusterConfig:
    """Tunable cluster-wide parameters (Master-side)."""

    heartbeat_interval_sec: float = 2.0          # worker -> master cadence
    heartbeat_timeout_sec: float = 80.0          # mark worker dead after this silence
    task_timeout_sec: float = 300.0              # kill a task stuck longer than this
    max_attempts: int = 3                        # per-task retry budget
    retry_backoff_base_sec: float = 1.0          # exponential backoff base
    speculative_execution: bool = True           # launch a duplicate for stragglers
    speculation_threshold: float = 2.0           # straggler = x median duration
    shuffle_fetch_batch: int = 64                # partitions fetched per HTTP round
    shuffle_spill_records: int = 20000           # external-sort spill threshold
    map_parallelism_factor: float = 3.0          # map tasks ~ workers * factor
    reduce_parallelism_factor: float = 2.0
    scheduler_tick_sec: float = 5.0              # master scheduling loop cadence
    metric_interval_sec: float = 2.0             # metric sample cadence
    demo_mode: bool = False                      # simulate work for fast UI demos
    default_input_rows: int = 12000              # generated input size for sample jobs
    seed: int = 20260930

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "ClusterConfig":
        known = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in d.items() if k in known})

    def validated(self) -> "ClusterConfig":
        """Return a copy with every field clamped into a safe range."""
        return ClusterConfig(
            heartbeat_interval_sec=_num(self.heartbeat_interval_sec, 2.0, 0.2, 60.0),
            heartbeat_timeout_sec=_num(self.heartbeat_timeout_sec, 8.0, 1.0, 300.0),
            task_timeout_sec=_num(self.task_timeout_sec, 300.0, 5.0, 3600.0),
            max_attempts=_int(self.max_attempts, 3, 1, 10),
            retry_backoff_base_sec=_num(self.retry_backoff_base_sec, 1.0, 0.1, 60.0),
            speculative_execution=_bool(self.speculative_execution, True),
            speculation_threshold=_num(self.speculation_threshold, 2.0, 1.0, 10.0),
            shuffle_fetch_batch=_int(self.shuffle_fetch_batch, 64, 1, 10000),
            shuffle_spill_records=_int(self.shuffle_spill_records, 20000, 100, 1_000_000),
            map_parallelism_factor=_num(self.map_parallelism_factor, 3.0, 0.5, 50.0),
            reduce_parallelism_factor=_num(self.reduce_parallelism_factor, 2.0, 0.5, 50.0),
            scheduler_tick_sec=_num(self.scheduler_tick_sec, 0.5, 0.05, 10.0),
            metric_interval_sec=_num(self.metric_interval_sec, 2.0, 0.5, 60.0),
            demo_mode=_bool(self.demo_mode, False),
            default_input_rows=_int(self.default_input_rows, 12000, 10, 10_000_000),
            seed=_int(self.seed, 20260930, 0, 2 ** 31 - 1),
        )


@dataclass
class JobDefaults:
    """Default job parameters shown on the submit page."""

    mapper: str = "wordcount_mapper"
    reducer: str = "wordcount_reducer"
    num_map_tasks: int = 8
    num_reduce_tasks: int = 4
    input_rows: int = 12000
    params: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "JobDefaults":
        known = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in d.items() if k in known})

    def validated(self) -> "JobDefaults":
        return JobDefaults(
            mapper=str(self.mapper or "wordcount_mapper"),
            reducer=str(self.reducer or "wordcount_reducer"),
            num_map_tasks=_int(self.num_map_tasks, 8, 1, 1000),
            num_reduce_tasks=_int(self.num_reduce_tasks, 4, 1, 500),
            input_rows=_int(self.input_rows, 12000, 10, 10_000_000),
            params=dict(self.params or {}),
        )


class ConfigManager:
    """Load/save the two config documents under ``data/config/``."""

    def __init__(self, storage: Storage) -> None:
        self.storage = storage

    # -- cluster ------------------------------------------------------
    def load_cluster(self) -> ClusterConfig:
        doc = self.storage.read("config", "cluster.json", default={})
        return ClusterConfig.from_dict(doc) if doc else ClusterConfig()

    def save_cluster(self, cfg: ClusterConfig) -> ClusterConfig:
        validated = cfg.validated()
        self.storage.write(validated.to_dict(), "config", "cluster.json")
        return validated

    # -- job defaults -------------------------------------------------
    def load_defaults(self) -> JobDefaults:
        doc = self.storage.read("config", "job_defaults.json", default={})
        return JobDefaults.from_dict(doc) if doc else JobDefaults()

    def save_defaults(self, defaults: JobDefaults) -> JobDefaults:
        validated = defaults.validated()
        self.storage.write(validated.to_dict(), "config", "job_defaults.json")
        return validated

    # -- combined -----------------------------------------------------
    def all(self) -> dict:
        return {
            "cluster": self.load_cluster().to_dict(),
            "job_defaults": self.load_defaults().to_dict(),
        }

    def ensure_seeded(self) -> None:
        """Create the config files on first boot if absent."""
        if self.storage.read("config", "cluster.json") is None:
            self.save_cluster(ClusterConfig())
        if self.storage.read("config", "job_defaults.json") is None:
            self.save_defaults(JobDefaults())
