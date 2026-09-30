"""Worker HTTP server.

A Worker exposes a small HTTP surface that the Master drives:

* ``POST /task/execute``  — accept a map/reduce task spec and start executing;
* ``POST /task/cancel``   — best-effort cancellation of a running task;
* ``GET  /shuffle/{job}/{task}/part-XXXX.jsonl`` — serve a shuffle partition to
  a reducer pulling it over HTTP;
* ``POST /shuffle/cleanup/{job}`` — drop a job's local shuffle data;
* ``GET  /health`` / ``GET /resource`` / ``GET /tasks`` — introspection.

The worker also registers with the Master on start and keeps a heartbeat thread
running so the Master can track liveness and load.
"""

from __future__ import annotations

import time
from typing import Optional

from flask import Flask, jsonify, request

from backend.common import constants as C
from backend.common.config import ClusterConfig
from backend.common.http_client import HttpClient
from backend.common.ids import new_id
from backend.common.jsonutil import now_ms
from backend.common.storage import Storage
from backend.worker.executor import Executor
from backend.worker.heartbeat import HeartbeatThread
from backend.worker.resource import ResourceSampler
from backend.worker.shuffle_store import ShuffleStore, parse_partition_index


class WorkerServer:
    def __init__(
        self,
        data_root: str,
        master_url: str,
        host: str = "127.0.0.1",
        port: int = 8001,
        name: str = "",
        worker_id: Optional[str] = None,
        config: Optional[ClusterConfig] = None,
        exec_mode: str = "process",
    ) -> None:
        self.host = host
        self.port = port
        self.worker_id = worker_id or new_id("worker")
        self.name = name or f"worker-{port}"
        self.master_url = master_url.rstrip("/")
        self.config = config or ClusterConfig()
        self.exec_mode = exec_mode

        self.storage = Storage(data_root)
        self.resource = ResourceSampler()
        self.shuffle = ShuffleStore(data_root)
        self.executor = Executor(
            self.worker_id, data_root, self.master_url, self.config, exec_mode,
        )
        self.heartbeat = HeartbeatThread(
            self.worker_id, self.master_url,
            self.config.heartbeat_timeout_sec, self._status_payload,
        )
        self.client = HttpClient(timeout=5.0, retries=1)
        self.registered = False

        self.app = Flask(f"worker-{port}")
        self._register_routes()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def _status_payload(self) -> dict:
        res = self.resource.sample()
        return {
            "cpu_percent": res["cpu_percent"],
            "mem_percent": res["mem_percent"],
            "load1": round(res["load1"] * 10.0, 2),
            "cpu_cores": res["cpu_cores"],
            "mem_total_mb": res["mem_total_mb"],
            "running_tasks": self.executor.running_count,
            "queued_tasks": 0,
        }

    def register(self) -> bool:
        payload = {
            "worker_id": self.worker_id,
            "name": self.name,
            "host": self.host,
            "port": self.port,
            "cpu_cores": self.resource.cpu_cores(),
            "mem_total_mb": self.resource.mem_total_mb(),
            "exec_mode": self.exec_mode,
        }
        resp = self.client.post(f"{self.master_url}/api/workers/register", payload, timeout=5.0)
        self.registered = resp.ok
        return self.registered

    def start(self) -> None:
        """Register with the Master (with retries) and start the heartbeat."""
        for attempt in range(20):
            if self.register():
                break
            time.sleep(1.0)
        self.heartbeat.start()

    def serve(self) -> None:
        self.app.run(host=self.host, port=self.port, threaded=True, use_reloader=False)

    # ------------------------------------------------------------------
    # Routes
    # ------------------------------------------------------------------
    def _register_routes(self) -> None:
        app = self.app
        app.add_url_rule("/health", "health", self._health, methods=["GET"])
        app.add_url_rule("/resource", "resource", self._resource, methods=["GET"])
        app.add_url_rule("/tasks", "tasks", self._tasks, methods=["GET"])
        app.add_url_rule("/task/execute", "execute", self._execute, methods=["POST"])
        app.add_url_rule("/task/cancel", "cancel", self._cancel, methods=["POST"])
        app.add_url_rule(
            "/shuffle/<job_id>/<task_id>/<partition_name>",
            "shuffle", self._get_shuffle, methods=["GET"],
        )
        app.add_url_rule(
            "/shuffle/cleanup/<job_id>", "shuffle_cleanup", self._cleanup, methods=["POST"],
        )

    def _health(self):
        return jsonify({
            "ok": True,
            "worker_id": self.worker_id,
            "name": self.name,
            "running_tasks": self.executor.running_count,
            "exec_mode": self.exec_mode,
        })

    def _resource(self):
        return jsonify(self.resource.sample())

    def _tasks(self):
        return jsonify({"running": self.executor.running_task_ids()})

    def _execute(self):
        spec = request.get_json(silent=True) or {}
        required = ("task_id", "job_id", "kind")
        if not all(k in spec for k in required):
            return jsonify({"accepted": False, "error": "missing task fields"}), 400
        # Reject unknown task kinds defensively.
        if spec["kind"] not in (C.TASK_MAP, C.TASK_REDUCE):
            return jsonify({"accepted": False, "error": "bad kind"}), 400
        accepted = self.executor.start_task(spec)
        if accepted:
            self._emit_log(spec, C.LOG_INFO, f"task accepted ({spec['kind']})")
        return jsonify({"accepted": accepted, "worker_id": self.worker_id})

    def _cancel(self):
        body = request.get_json(silent=True) or {}
        task_id = body.get("task_id", "")
        return jsonify({"cancelled": self.executor.cancel(task_id) if task_id else False})

    def _get_shuffle(self, job_id: str, task_id: str, partition_name: str):
        index = parse_partition_index(partition_name)
        pairs = self.shuffle.read_partition(job_id, task_id, index)
        return jsonify(pairs)

    def _cleanup(self, job_id: str):
        self.shuffle.cleanup(job_id, "")
        return jsonify({"cleaned": job_id})

    # ------------------------------------------------------------------
    # Worker-side logging (forwarded to the Master for aggregation)
    # ------------------------------------------------------------------
    def _emit_log(self, spec: dict, level: str, message: str) -> None:
        try:
            self.client.post(f"{self.master_url}/api/workers/log", {
                "worker_id": self.worker_id,
                "job_id": spec.get("job_id", ""),
                "task_id": spec.get("task_id", ""),
                "stage": "task",
                "level": level,
                "message": message,
                "ts_ms": now_ms(),
            }, timeout=5.0)
        except Exception:
            pass
