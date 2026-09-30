"""Central constants: states, stage names, kinds, log levels, and labels.

Every serialized value lives here so the Master, Worker and frontend share a
single vocabulary.  Values are plain strings so they round-trip through JSON
without any enum decoding ceremony.
"""

# ---------------------------------------------------------------------------
# Job lifecycle states
# ---------------------------------------------------------------------------
JOB_PENDING = "PENDING"        # accepted, not yet scheduled
JOB_SHARDING = "SHARDING"      # input is being split into shards
JOB_MAP = "MAP"                # map tasks running
JOB_SHUFFLE = "SHUFFLE"        # partitions being transferred / sorted
JOB_REDUCE = "REDUCE"          # reduce tasks running
JOB_SUCCEEDED = "SUCCEEDED"
JOB_FAILED = "FAILED"
JOB_CANCELLED = "CANCELLED"

JOB_ACTIVE_STATES = {JOB_PENDING, JOB_SHARDING, JOB_MAP, JOB_SHUFFLE, JOB_REDUCE}
JOB_TERMINAL_STATES = {JOB_SUCCEEDED, JOB_FAILED, JOB_CANCELLED}

JOB_STATES = [
    JOB_PENDING, JOB_SHARDING, JOB_MAP, JOB_SHUFFLE, JOB_REDUCE,
    JOB_SUCCEEDED, JOB_FAILED, JOB_CANCELLED,
]

# ---------------------------------------------------------------------------
# Task states
# ---------------------------------------------------------------------------
TASK_PENDING = "PENDING"
TASK_ASSIGNED = "ASSIGNED"     # dispatched to a worker, not yet acknowledged
TASK_RUNNING = "RUNNING"
TASK_SUCCEEDED = "SUCCEEDED"
TASK_FAILED = "FAILED"         # permanently failed after max attempts
TASK_RETRYING = "RETRYING"     # failed, waiting to be re-dispatched

TASK_ACTIVE_STATES = {TASK_ASSIGNED, TASK_RUNNING, TASK_RETRYING}
TASK_TERMINAL_STATES = {TASK_SUCCEEDED, TASK_FAILED}

# ---------------------------------------------------------------------------
# Stage names (also used as directory names in the JSON store)
# ---------------------------------------------------------------------------
STAGE_INPUT = "input"
STAGE_MAP = "map"
STAGE_SHUFFLE = "shuffle"
STAGE_REDUCE = "reduce"

STAGES = [STAGE_INPUT, STAGE_MAP, STAGE_SHUFFLE, STAGE_REDUCE]

# Ordered run stages (input is produced at submission time).
RUN_STAGES = [STAGE_MAP, STAGE_SHUFFLE, STAGE_REDUCE]

# ---------------------------------------------------------------------------
# Task kinds
# ---------------------------------------------------------------------------
TASK_MAP = "map"
TASK_REDUCE = "reduce"

# ---------------------------------------------------------------------------
# Worker states
# ---------------------------------------------------------------------------
WORKER_ALIVE = "alive"
WORKER_DEAD = "dead"

# ---------------------------------------------------------------------------
# Log levels
# ---------------------------------------------------------------------------
LOG_DEBUG = "DEBUG"
LOG_INFO = "INFO"
LOG_WARN = "WARN"
LOG_ERROR = "ERROR"
LOG_LEVELS = [LOG_DEBUG, LOG_INFO, LOG_WARN, LOG_ERROR]

# ---------------------------------------------------------------------------
# Bilingual labels shared by the backend (also mirrored in the frontend)
# ---------------------------------------------------------------------------
STATE_LABELS = {
    JOB_PENDING: "待调度 Pending",
    JOB_SHARDING: "分片中 Sharding",
    JOB_MAP: "Map 阶段 Map",
    JOB_SHUFFLE: "Shuffle 阶段 Shuffle",
    JOB_REDUCE: "Reduce 阶段 Reduce",
    JOB_SUCCEEDED: "成功 Succeeded",
    JOB_FAILED: "失败 Failed",
    JOB_CANCELLED: "已取消 Cancelled",
    TASK_PENDING: "待执行 Pending",
    TASK_ASSIGNED: "已分配 Assigned",
    TASK_RUNNING: "运行中 Running",
    TASK_SUCCEEDED: "成功 Succeeded",
    TASK_FAILED: "失败 Failed",
    TASK_RETRYING: "重试中 Retrying",
    WORKER_ALIVE: "存活 Alive",
    WORKER_DEAD: "失联 Dead",
}

STAGE_LABELS = {
    STAGE_INPUT: "输入分片 Input",
    STAGE_MAP: "Map 阶段 Map",
    STAGE_SHUFFLE: "Shuffle 阶段 Shuffle",
    STAGE_REDUCE: "Reduce 阶段 Reduce",
}

LOG_LEVEL_LABELS = {
    LOG_DEBUG: "调试 DEBUG",
    LOG_INFO: "信息 INFO",
    LOG_WARN: "警告 WARN",
    LOG_ERROR: "错误 ERROR",
}


def state_label(state: str) -> str:
    """Human readable bilingual label for a state, falling back to the raw value."""
    return STATE_LABELS.get(state, str(state))


def stage_label(stage: str) -> str:
    return STAGE_LABELS.get(stage, str(stage))
