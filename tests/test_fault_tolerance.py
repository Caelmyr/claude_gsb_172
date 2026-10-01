"""Tests for fault tolerance / retry, and map+reduce task correctness."""

import collections
import shutil
import tempfile
import time
import unittest

from backend.common.config import ClusterConfig
from backend.common.logbus import LogBus
from backend.common.storage import Storage
from backend.master.fault_tolerance import FaultTolerance
from backend.master.job_manager import JobManager
from backend.master.metrics import Metrics
from backend.master.registry import WorkerRegistry
from backend.master.scheduler import Scheduler
from backend.master.shuffle import ShuffleCoordinator
from backend.common.models import new_worker
from backend.tasks.registry import get_reducer, register_mapper


def _sleep_mapper(records, params):
    time.sleep(float(params.get("sleep_sec", 10.0)))
    return []


register_mapper("test_sleep_mapper", _sleep_mapper)
from backend.tasks.samples import generate_input_records
from backend.worker.executor import _run_map
from backend.worker.shuffle_store import ShuffleStore


class TestFaultTolerance(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = Storage(self.tmp)
        self.config = ClusterConfig(max_attempts=2)
        self.jm = JobManager(self.storage, self.config, LogBus(self.storage))
        self.job = self.jm.submit({
            "name": "t", "mapper": "wordcount_mapper", "reducer": "count_reducer",
            "num_map_tasks": 3, "num_reduce_tasks": 2, "input_rows": 100, "params": {},
        })
        self.ft = FaultTolerance(self.storage, self.jm, self.config, LogBus(self.storage))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_retry_then_permanent_failure(self):
        task = self.jm.tasks_for(self.job.job_id, "map")[0]
        # First failure -> retry.
        self.assertTrue(self.ft.handle_task_failure(self.job, task, "boom"))
        task = self.jm.get_task(self.job.job_id, task.task_id)
        self.assertEqual(task.status, "RETRYING")
        self.assertEqual(task.attempts, 1)
        # Exhaust retries until the job fails.
        retried = True
        while retried:
            retried = self.ft.handle_task_failure(self.job, task, "boom")
            task = self.jm.get_task(self.job.job_id, task.task_id)
        self.assertEqual(task.status, "FAILED")
        self.assertEqual(self.jm.get_job(self.job.job_id).status, "FAILED")

    def test_fault_event_recorded(self):
        task = self.jm.tasks_for(self.job.job_id, "map")[0]
        self.ft.handle_task_failure(self.job, task, "boom")
        faults = self.ft.list_faults(self.job.job_id)
        self.assertEqual(len(faults), 1)
        self.assertEqual(faults[0]["kind"], "task_failed")


class TestTaskTimeout(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = Storage(self.tmp)
        self.config = ClusterConfig(task_timeout_sec=0.05, scheduler_tick_sec=0.05,
                                    max_attempts=2, retry_backoff_base_sec=0.01)
        self.logbus = LogBus(self.storage)
        self.jm = JobManager(self.storage, self.config, self.logbus)
        self.registry = WorkerRegistry(self.storage, self.config)
        self.ft = FaultTolerance(self.storage, self.jm, self.config, self.logbus)
        self.shuffle = ShuffleCoordinator(self.storage, self.jm, self.registry, self.logbus)
        self.scheduler = Scheduler(
            self.storage, self.jm, self.registry, self.shuffle, self.ft,
            Metrics(self.storage), self.config, self.logbus,
        )
        self.scheduler.client.post = lambda *args, **kwargs: None
        self.worker = self.registry.register(new_worker(
            "w-timeout", "w-timeout", "127.0.0.1", 19999, 2, 512,
        ).to_dict())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _submit(self, name: str):
        return self.jm.submit({
            "name": name, "mapper": "wordcount_mapper", "reducer": "count_reducer",
            "num_map_tasks": 1, "num_reduce_tasks": 1, "input_rows": 10, "params": {},
        })

    def test_expired_attempt_retries_and_does_not_fail_another_job(self):
        job_a = self._submit("a")
        job_b = self._submit("b")
        task_a = self.jm.tasks_for(job_a.job_id, "map")[0]
        task_b = self.jm.tasks_for(job_b.job_id, "map")[0]
        self.assertEqual(task_a.task_id, task_b.task_id)

        # Mark both as accepted attempts, but make only A old enough to expire.
        now_ms = time.time() * 1000
        for task, job, age in ((task_a, job_a, 100), (task_b, job_b, 0)):
            assigned = int(now_ms - age)
            self.jm.apply_task(job.job_id, task.task_id, lambda t: (
                setattr(t, "status", "RUNNING"),
                setattr(t, "worker_id", "w-timeout"),
                setattr(t, "execution_id", f"exec-{job.job_id}"),
                setattr(t, "assigned_ms", assigned),
                setattr(t, "started_ms", assigned),
                t.stats.__setitem__("executions", [{
                    "execution_id": f"exec-{job.job_id}",
                    "worker_id": "w-timeout",
                    "status": "RUNNING",
                    "assigned_ms": assigned,
                    "started_ms": assigned,
                }]),
            ))

        self.scheduler._enforce_task_timeouts(job_a)
        task_a = self.jm.get_task(job_a.job_id, task_a.task_id)
        task_b = self.jm.get_task(job_b.job_id, task_b.task_id)
        self.assertEqual(task_a.status, "RETRYING")
        self.assertEqual(task_a.attempts, 1)
        self.assertEqual(task_b.status, "RUNNING")
        self.assertEqual(task_b.attempts, 0)
        self.assertNotIn(f"exec-{job_b.job_id}", task_a.execution_id)

    def test_timeout_setting_change_is_used_on_next_tick(self):
        job = self._submit("dynamic-timeout")
        task = self.jm.tasks_for(job.job_id, "map")[0]
        assigned = int(time.time() * 1000 - 80)
        self.jm.apply_task(job.job_id, task.task_id, lambda t: (
            setattr(t, "status", "RUNNING"),
            setattr(t, "worker_id", "w-timeout"),
            setattr(t, "execution_id", "exec-dynamic"),
            setattr(t, "assigned_ms", assigned),
            setattr(t, "started_ms", assigned),
            t.stats.__setitem__("executions", [{
                "execution_id": "exec-dynamic",
                "worker_id": "w-timeout",
                "status": "RUNNING",
                "assigned_ms": assigned,
                "started_ms": assigned,
            }]),
        ))

        # 0.5s configured: an 80ms-old task must remain untouched.
        self.config.task_timeout_sec = 0.5
        self.scheduler._enforce_task_timeouts(job)
        task = self.jm.get_task(job.job_id, task.task_id)
        self.assertEqual(task.status, "RUNNING")

        # Lowering the shared config object is what the PUT /api/config path does.
        self.config.task_timeout_sec = 0.05
        self.scheduler._enforce_task_timeouts(job)
        task = self.jm.get_task(job.job_id, task.task_id)
        self.assertEqual(task.status, "RETRYING")

    def test_last_expired_attempt_fails_job(self):
        self.config.max_attempts = 1
        job = self._submit("one-attempt")
        task = self.jm.tasks_for(job.job_id, "map")[0]
        assigned = int(time.time() * 1000 - 100)
        self.jm.apply_task(job.job_id, task.task_id, lambda t: (
            setattr(t, "status", "RUNNING"),
            setattr(t, "worker_id", "w-timeout"),
            setattr(t, "execution_id", "exec-one"),
            setattr(t, "assigned_ms", assigned),
            setattr(t, "started_ms", assigned),
            t.stats.__setitem__("executions", [{
                "execution_id": "exec-one",
                "worker_id": "w-timeout",
                "status": "RUNNING",
                "assigned_ms": assigned,
                "started_ms": assigned,
            }]),
        ))

        self.scheduler._enforce_task_timeouts(job)
        task = self.jm.get_task(job.job_id, task.task_id)
        self.assertEqual(task.status, "FAILED")
        self.assertEqual(self.jm.get_job(job.job_id).status, "FAILED")

    def test_worker_allows_same_task_id_from_different_jobs(self):
        from backend.worker.executor import Executor

        executor = Executor(
            "w-local", self.tmp, "http://127.0.0.1:9",
            ClusterConfig(task_timeout_sec=5.0), exec_mode="process",
        )
        base = {
            "task_id": "m-0000",
            "kind": "map",
            "mapper": "test_sleep_mapper",
            "reducer": "count_reducer",
            "params": {"sleep_sec": 5.0},
            "partition_count": 1,
            "records": ["stuck"],
            "timeout_sec": 5.0,
        }
        first = dict(base, job_id="job-a", execution_id="exec-a")
        second = dict(base, job_id="job-b", execution_id="exec-b")
        duplicate = dict(base, job_id="job-a", execution_id="exec-a")
        try:
            self.assertTrue(executor.start_task(first))
            self.assertTrue(executor.start_task(second))
            self.assertFalse(executor.start_task(duplicate))
        finally:
            executor.shutdown()
            time.sleep(0.1)

    def test_configured_timeout_is_enforced_for_stuck_process(self):
        # The worker's own watchdog is important when the master cannot reach it
        # quickly; it terminates the OS process instead of waiting forever.
        from backend.worker.executor import Executor

        executor = Executor(
            "w-local", self.tmp, "http://127.0.0.1:9",
            ClusterConfig(task_timeout_sec=0.05), exec_mode="process",
        )
        accepted = executor.start_task({
            "task_id": "m-0000",
            "job_id": "local-timeout",
            "execution_id": "exec-local-timeout",
            "kind": "map",
            "mapper": "test_sleep_mapper",
            "reducer": "count_reducer",
            "params": {"sleep_sec": 10.0},
            "partition_count": 1,
            "records": ["stuck"],
            "timeout_sec": 0.05,
            "spill_records": 1000000,
            "tmp_dir": self.tmp,
        })
        self.assertTrue(accepted)
        deadline = time.time() + 3.0
        while executor.running_count and time.time() < deadline:
            time.sleep(0.02)
        self.assertEqual(executor.running_count, 0)


class TestMapReduceCorrectness(unittest.TestCase):
    def test_wordcount_matches_reference(self):
        tmp = tempfile.mkdtemp()
        records = generate_input_records("wordcount", 800, seed=7)
        reference = collections.Counter()
        for line in records:
            for w in line.lower().split():
                reference[w] += 1

        spec = {
            "task_id": "m-0000", "job_id": "job", "kind": "map",
            "mapper": "wordcount_mapper", "reducer": "count_reducer",
            "params": {}, "partition_count": 4, "records": records,
            "spill_records": 300, "tmp_dir": tmp,
        }
        _run_map(spec, tmp, lambda p, a, b: None)

        store = ShuffleStore(tmp)
        grouped = collections.defaultdict(list)
        for p in range(4):
            for k, v in store.read_partition("job", "m-0000", p):
                grouped[k].append(v)

        reducer = get_reducer("count_reducer")
        result = {k: reducer(k, vs, {})["count"] for k, vs in grouped.items()}
        self.assertEqual(dict(result), dict(reference))
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
