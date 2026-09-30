"""Tests for fault tolerance / retry, and map+reduce task correctness."""

import collections
import shutil
import tempfile
import unittest

from backend.common.config import ClusterConfig
from backend.common.logbus import LogBus
from backend.common.storage import Storage
from backend.master.fault_tolerance import FaultTolerance
from backend.master.job_manager import JobManager
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
