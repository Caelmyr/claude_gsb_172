"""Tests for atomic JSON storage, locking, versioning and merging (difficulty point 5)."""

import os
import shutil
import tempfile
import threading
import unittest

from backend.common.storage import (
    AtomicJsonStore, Storage, VersionConflict, append_jsonl, atomic_write_json,
    merge_jsonl_files, read_json, read_jsonl,
)


class TestAtomicJson(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_atomic_write_and_version_increment(self):
        path = os.path.join(self.tmp, "a", "b.json")
        atomic_write_json(path, {"x": 1})
        doc = read_json(path)
        self.assertEqual(doc["x"], 1)
        self.assertEqual(doc["_version"], 1)
        atomic_write_json(path, {"x": 2})
        self.assertEqual(read_json(path)["_version"], 2)

    def test_missing_file_returns_default(self):
        self.assertIsNone(read_json(os.path.join(self.tmp, "nope.json")))
        self.assertEqual(read_json(os.path.join(self.tmp, "nope.json"), default={}), {})

    def test_corrupt_file_returns_default(self):
        path = os.path.join(self.tmp, "bad.json")
        with open(path, "w") as f:
            f.write("{ not json")
        self.assertEqual(read_json(path, default=[]), [])

    def test_append_and_read_jsonl(self):
        path = os.path.join(self.tmp, "logs", "t.jsonl")
        for i in range(20):
            append_jsonl(path, {"i": i})
        self.assertEqual(len(read_jsonl(path)), 20)

    def test_merge_jsonl_sorted(self):
        a = os.path.join(self.tmp, "a.jsonl")
        b = os.path.join(self.tmp, "b.jsonl")
        append_jsonl(a, ["b", 2])
        append_jsonl(a, ["a", 1])
        append_jsonl(b, ["c", 3])
        out = os.path.join(self.tmp, "out.jsonl")
        n = merge_jsonl_files([a, b], out, key=lambda r: r[0])
        self.assertEqual(n, 3)
        self.assertEqual([r[0] for r in read_jsonl(out)], ["a", "b", "c"])

    def test_version_conflict_rejected(self):
        store = AtomicJsonStore()
        path = os.path.join(self.tmp, "doc.json")
        store.write(path, {"v": 0})
        stale = store.read(path)
        store.write(path, {"v": 1})  # someone else updates
        with self.assertRaises(VersionConflict):
            store.update(path, lambda d: d, if_version=stale["_version"])

    def test_concurrent_appends_no_loss(self):
        path = os.path.join(self.tmp, "c.jsonl")
        def writer(n):
            for i in range(100):
                append_jsonl(path, {"n": n, "i": i})
        threads = [threading.Thread(target=writer, args=(k,)) for k in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(read_jsonl(path)), 400)

    def test_storage_wrapper(self):
        storage = Storage(self.tmp)
        storage.write({"a": 1}, "cfg", "c.json")
        self.assertEqual(storage.read("cfg", "c.json")["a"], 1)
        storage.append({"line": 1}, "l.jsonl")
        self.assertEqual(len(storage.read_lines("l.jsonl")), 1)


if __name__ == "__main__":
    unittest.main()
