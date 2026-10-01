"""Tests for fault tolerance / retry, and map+reduce task correctness."""

import collections
import shutil
import tempfile
import unittest

from backend.common.config import ClusterConfig
from backend.common.jsonutil import now_ms
from backend.common.logbus import LogBus
from backend.common.storage import Storage
from backend.master.fault_tolerance import FaultTolerance
from backend.master.job_manager import JobManager
from backend.master.registry import WorkerRegistry
from backend.tasks.registry import get_reducer
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
    """``task_timeout_sec`` must be enforced: stuck attempts are detected on
    their own per-attempt clock, retried with real backoff, and never
    re-flagged early after a retry."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = Storage(self.tmp)
        self.config = ClusterConfig(max_attempts=2, task_timeout_sec=10.0,
                                    retry_backoff_base_sec=1.0)
        self.jm = JobManager(self.storage, self.config, LogBus(self.storage))
        self.job = self.jm.submit({
            "name": "t", "mapper": "wordcount_mapper", "reducer": "count_reducer",
            "num_map_tasks": 3, "num_reduce_tasks": 2, "input_rows": 100, "params": {},
        })
        self.ft = FaultTolerance(self.storage, self.jm, self.config, LogBus(self.storage))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _task(self, index=0):
        return self.jm.tasks_for(self.job.job_id, "map")[index]

    def test_running_task_past_timeout_is_flagged(self):
        task = self._task()
        now = now_ms()
        self.jm.update_task(self.job.job_id, task.task_id, status="RUNNING",
                            worker_id="w1", assigned_ms=now - 60_000,
                            started_ms=now - 30_000)
        flagged = self.ft.find_timed_out(self.job)
        self.assertEqual([t.task_id for t in flagged], [task.task_id])

    def test_recent_task_is_not_flagged(self):
        task = self._task()
        now = now_ms()
        self.jm.update_task(self.job.job_id, task.task_id, status="RUNNING",
                            worker_id="w1", assigned_ms=now - 9_000,
                            started_ms=now - 8_000)
        self.assertEqual(self.ft.find_timed_out(self.job), [])

    def test_assigned_but_never_started_uses_dispatch_clock(self):
        task = self._task()
        self.jm.update_task(self.job.job_id, task.task_id, status="ASSIGNED",
                            worker_id="w1", assigned_ms=now_ms() - 30_000)
        self.assertEqual([t.task_id for t in self.ft.find_timed_out(self.job)],
                         [task.task_id])

    def test_pending_and_succeeded_tasks_are_ignored(self):
        self.assertEqual(self.ft.find_timed_out(self.job), [])

    def test_timeout_retry_resets_attempt_clock(self):
        task = self._task()
        self.jm.update_task(self.job.job_id, task.task_id, status="RUNNING",
                            worker_id="w1", started_ms=now_ms() - 30_000)
        retried = self.ft.handle_task_failure(self.job, task, "timed out after 10s",
                                              "w1", kind="task_timeout")
        self.assertTrue(retried)
        task = self.jm.get_task(self.job.job_id, task.task_id)
        self.assertEqual(task.status, "RETRYING")
        self.assertEqual(task.started_ms, 0)
        self.assertEqual(task.assigned_ms, 0)
        # The re-queued task must not be immediately re-flagged on the old clock.
        self.assertEqual(self.ft.find_timed_out(self.job), [])
        # And the event is recorded under the timeout kind.
        faults = self.ft.list_faults(self.job.job_id)
        self.assertEqual(faults[-1]["kind"], "task_timeout")

    def test_timeout_exhaustion_fails_job(self):
        task = self._task()
        self.jm.update_task(self.job.job_id, task.task_id, status="RUNNING",
                            worker_id="w1", started_ms=now_ms() - 30_000)
        while self.ft.handle_task_failure(self.job, task, "timed out after 10s",
                                          "w1", kind="task_timeout"):
            task = self.jm.get_task(self.job.job_id, task.task_id)
        self.assertEqual(self.jm.get_job(self.job.job_id).status, "FAILED")

    def test_retry_backoff_is_milliseconds(self):
        task = self._task()
        before = now_ms()
        self.ft.handle_task_failure(self.job, task, "boom")
        task = self.jm.get_task(self.job.job_id, task.task_id)
        # retry_backoff_base_sec = 1.0 -> first backoff ~1000 ms, not 1 ms.
        self.assertGreaterEqual(task.retry_after_ms - before, 900)


class TestWorkerReap(unittest.TestCase):
    """``heartbeat_timeout_sec`` is in seconds; reaping must not take minutes."""

    def test_reap_uses_seconds(self):
        tmp = tempfile.mkdtemp()
        try:
            registry = WorkerRegistry(Storage(tmp),
                                      ClusterConfig(heartbeat_timeout_sec=1.0))
            registry.register({"worker_id": "w1", "name": "w1", "port": 1})
            worker = registry.get("w1")
            worker.last_heartbeat_ms = now_ms() - 2_000  # 2 s of silence
            dead = registry.reap()
            self.assertEqual([w.worker_id for w in dead], ["w1"])
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


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
