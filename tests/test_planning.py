"""Tests for deterministic hashing, shard planning and job submission."""

import shutil
import subprocess
import sys
import tempfile
import unittest

from backend.common.config import ClusterConfig
from backend.common.hashing import partition_for, stable_hash
from backend.common.logbus import LogBus
from backend.common.storage import Storage
from backend.master.job_manager import JobManager
from backend.master.shard_planner import split_evenly


class TestHashing(unittest.TestCase):
    def test_deterministic(self):
        self.assertEqual(partition_for("the", 4), partition_for("the", 4))
        self.assertEqual(stable_hash("key"), stable_hash("key"))

    def test_in_range(self):
        for i in range(200):
            self.assertTrue(0 <= partition_for(f"k{i}", 7) < 7)

    def test_stable_across_processes(self):
        code = "from backend.common.hashing import partition_for; print(partition_for('word', 8))"
        out = subprocess.check_output([sys.executable, "-c", code], text=True).strip()
        self.assertEqual(int(out), partition_for("word", 8))


class TestSplitEvenly(unittest.TestCase):
    def test_sizes_differ_by_at_most_one(self):
        chunks = split_evenly(list(range(10)), 3)
        self.assertEqual(sorted(len(c) for c in chunks), [3, 3, 4])

    def test_empty(self):
        chunks = split_evenly([], 4)
        self.assertEqual(len(chunks), 4)
        self.assertTrue(all(len(c) == 0 for c in chunks))

    def test_more_parts_than_items(self):
        chunks = split_evenly([1, 2], 10)
        self.assertEqual(sum(len(c) for c in chunks), 2)


class TestSubmit(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = Storage(self.tmp)
        self.jm = JobManager(self.storage, ClusterConfig(), LogBus(self.storage))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_submit_creates_shards_and_tasks(self):
        job = self.jm.submit({
            "name": "t", "mapper": "wordcount_mapper", "reducer": "count_reducer",
            "num_map_tasks": 4, "num_reduce_tasks": 2, "input_rows": 800, "params": {},
        })
        self.assertEqual(job.status, "MAP")
        self.assertEqual(len(job.map_task_ids), 4)
        self.assertEqual(len(job.reduce_task_ids), 2)
        self.assertEqual(len(self.jm.tasks_for(job.job_id)), 6)
        # persisted round-trip
        self.assertEqual(self.jm.get_job(job.job_id).name, "t")

    def test_submit_rejects_unknown_mapper(self):
        with self.assertRaises(ValueError):
            self.jm.submit({"name": "t", "mapper": "nope", "reducer": "count_reducer"})

    def test_granularity_clamps_map_tasks(self):
        job = self.jm.submit({
            "name": "t", "mapper": "wordcount_mapper", "reducer": "count_reducer",
            "num_map_tasks": 100, "num_reduce_tasks": 2, "input_rows": 30, "params": {},
        })
        # never more map tasks than input records
        self.assertLessEqual(len(job.map_task_ids), 30)


if __name__ == "__main__":
    unittest.main()
