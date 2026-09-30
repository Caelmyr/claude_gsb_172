"""Master HTTP server: REST API, static frontend hosting and wiring.

The Master exposes one REST surface for the browser (10 pages) and a second for
workers (register / heartbeat / task-status / task-complete / log).  All state
mutations funnel through the JobManager's locked helpers, and the scheduler runs
on a background thread, so Flask's threaded request handlers never corrupt
shared state.
"""

from __future__ import annotations

import csv
import io
import os
from typing import Optional

from flask import Flask, Response, jsonify, request, send_from_directory

from backend.common import constants as C
from backend.common.config import ClusterConfig, ConfigManager, JobDefaults
from backend.common.logbus import LogBus
from backend.common.models import Job
from backend.common.storage import Storage, list_files, read_json
from backend.master.fault_tolerance import FaultTolerance
from backend.master.job_manager import JobManager
from backend.master.metrics import Metrics
from backend.master.registry import WorkerRegistry
from backend.master.scheduler import Scheduler
from backend.master.shuffle import ShuffleCoordinator
from backend.tasks.registry import list_all as list_functions
from backend.tasks.samples import list_sample_jobs

FRONTEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "frontend"))


class Master:
    def __init__(self, data_root: str, host: str = "0.0.0.0", port: int = 8000,
                 config: Optional[ClusterConfig] = None) -> None:
        self.host = host
        self.port = port
        self.storage = Storage(data_root)

        self.config_manager = ConfigManager(self.storage)
        self.config_manager.ensure_seeded()
        self.config = (config or self.config_manager.load_cluster()).validated()

        self.logbus = LogBus(self.storage)
        self.job_manager = JobManager(self.storage, self.config, self.logbus)
        self.registry = WorkerRegistry(self.storage, self.config)
        self.metrics = Metrics(self.storage)
        self.shuffle = ShuffleCoordinator(self.storage, self.job_manager, self.registry, self.logbus)
        self.fault_tolerance = FaultTolerance(self.storage, self.job_manager, self.config, self.logbus)
        self.registry.on_death = self.fault_tolerance.handle_worker_death
        self.scheduler = Scheduler(
            self.storage, self.job_manager, self.registry, self.shuffle,
            self.fault_tolerance, self.metrics, self.config, self.logbus,
        )

        self.app = Flask("master", static_folder=FRONTEND_DIR, static_url_path="")
        self._register_routes()

    # ------------------------------------------------------------------
    def start(self) -> None:
        self.scheduler.start()

    def serve(self) -> None:
        self.app.run(host=self.host, port=self.port, threaded=True, use_reloader=False)

    # ------------------------------------------------------------------
    # Route registration
    # ------------------------------------------------------------------
    def _register_routes(self) -> None:
        app = self.app
        app.add_url_rule("/", "index", self._index)

        # --- browser-facing -------------------------------------------------
        app.add_url_rule("/api/overview", "overview", self._overview, methods=["GET"])
        app.add_url_rule("/api/functions", "functions", self._functions, methods=["GET"])
        app.add_url_rule("/api/samples", "samples", self._samples, methods=["GET"])
        app.add_url_rule("/api/jobs", "jobs", self._jobs, methods=["GET", "POST"])
        app.add_url_rule("/api/jobs/<job_id>", "job_detail", self._job_detail, methods=["GET"])
        app.add_url_rule("/api/jobs/<job_id>/cancel", "job_cancel", self._job_cancel, methods=["POST"])
        app.add_url_rule("/api/jobs/<job_id>/tasks", "job_tasks", self._job_tasks, methods=["GET"])
        app.add_url_rule("/api/jobs/<job_id>/shards", "job_shards", self._job_shards, methods=["GET"])
        app.add_url_rule("/api/jobs/<job_id>/shuffle", "job_shuffle", self._job_shuffle, methods=["GET"])
        app.add_url_rule("/api/jobs/<job_id>/logs", "job_logs", self._job_logs, methods=["GET"])
        app.add_url_rule("/api/jobs/<job_id>/metrics", "job_metrics", self._job_metrics, methods=["GET"])
        app.add_url_rule("/api/jobs/<job_id>/faults", "job_faults", self._job_faults, methods=["GET"])
        app.add_url_rule("/api/jobs/<job_id>/results", "job_results", self._job_results, methods=["GET"])
        app.add_url_rule("/api/jobs/<job_id>/results/download", "job_results_download",
                         self._job_results_download, methods=["GET"])
        app.add_url_rule("/api/workers", "workers", self._workers, methods=["GET"])
        app.add_url_rule("/api/workers/<worker_id>/metrics", "worker_metrics", self._worker_metrics, methods=["GET"])
        app.add_url_rule("/api/cluster/metrics", "cluster_metrics", self._cluster_metrics, methods=["GET"])
        app.add_url_rule("/api/config", "config", self._config, methods=["GET", "PUT"])
        app.add_url_rule("/api/config/defaults", "config_defaults", self._config_defaults, methods=["GET", "PUT"])

        # --- worker-facing --------------------------------------------------
        app.add_url_rule("/api/workers/register", "worker_register", self._worker_register, methods=["POST"])
        app.add_url_rule("/api/workers/heartbeat", "worker_heartbeat", self._worker_heartbeat, methods=["POST"])
        app.add_url_rule("/api/workers/task-status", "worker_task_status", self._worker_task_status, methods=["POST"])
        app.add_url_rule("/api/workers/task-complete", "worker_task_complete", self._worker_task_complete, methods=["POST"])
        app.add_url_rule("/api/workers/log", "worker_log", self._worker_log, methods=["POST"])

    # ------------------------------------------------------------------
    # Static
    # ------------------------------------------------------------------
    def _index(self):
        return send_from_directory(FRONTEND_DIR, "index.html")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _task_view(self, task) -> dict:
        d = task.to_dict()
        worker = self.registry.get(task.worker_id) if task.worker_id else None
        d["worker_name"] = worker.name if worker else ""
        d["status_label"] = C.state_label(task.status)
        return d

    def _get_job(self, job_id: str):
        job = self.job_manager.get_job(job_id)
        if job is None:
            return None, jsonify({"error": f"unknown job {job_id}"}), 404
        return job, None, None

    def _read_results(self, job: Job) -> list[dict]:
        records: list[dict] = []
        root = self.storage.path("jobs", job.job_id, "results", C.STAGE_REDUCE)
        for path in list_files(root, suffix=".json"):
            doc = read_json(path)
            if doc:
                for rec in doc.get("records", []):
                    records.append(rec)
        return records

    def _result_partitions(self, job: Job) -> list[dict]:
        out: list[dict] = []
        root = self.storage.path("jobs", job.job_id, "results", C.STAGE_REDUCE)
        for path in list_files(root, suffix=".json"):
            doc = read_json(path)
            if doc:
                out.append({
                    "partition": doc.get("partition"),
                    "partition_name": doc.get("partition_name"),
                    "count": doc.get("count", 0),
                    "task_id": doc.get("task_id"),
                })
        out.sort(key=lambda d: d.get("partition", 0))
        return out

    # ------------------------------------------------------------------
    # Browser-facing routes
    # ------------------------------------------------------------------
    def _overview(self):
        jobs = self.job_manager.list_jobs()
        active = [j for j in jobs if not j.is_terminal]
        return jsonify({
            "jobs_total": len(jobs),
            "jobs_active": len(active),
            "jobs_succeeded": sum(1 for j in jobs if j.status in (C.JOB_SUCCEEDED, C.JOB_REDUCE)),
            "jobs_failed": sum(1 for j in jobs if j.status == C.JOB_FAILED),
            "jobs_cancelled": sum(1 for j in jobs if j.status == C.JOB_CANCELLED),
            "workers": self.registry.summary(),
            "config": self.config.to_dict(),
            "recent_jobs": [self.job_manager.job_summary(j) for j in jobs[:10]],
        })

    def _functions(self):
        return jsonify(list_functions())

    def _samples(self):
        return jsonify(list_sample_jobs())

    def _jobs(self):
        if request.method == "POST":
            body = request.get_json(silent=True) or {}
            body["_defaults"] = self.config_manager.load_defaults().to_dict()
            try:
                job = self.job_manager.submit(body)
            except (ValueError, KeyError) as exc:
                return jsonify({"error": str(exc)}), 400
            return jsonify(self.job_manager.job_summary(job)), 201
        return jsonify({"jobs": [self.job_manager.job_summary(j) for j in self.job_manager.list_jobs()]})

    def _job_detail(self, job_id: str):
        job, err, code = self._get_job(job_id)
        if job is None:
            return err, code
        summary = self.job_manager.job_summary(job)
        summary["shuffle"] = self.shuffle.progress(job)
        summary["fault_count"] = len(self.fault_tolerance.list_faults(job_id))
        return jsonify({
            "job": summary,
            "tasks": [self._task_view(t) for t in self.job_manager.tasks_for(job_id)],
        })

    def _job_cancel(self, job_id: str):
        job, err, code = self._get_job(job_id)
        if job is None:
            return err, code
        if not job.is_terminal:
            self.job_manager.cancel(job)
        return jsonify(self.job_manager.job_summary(job))

    def _job_tasks(self, job_id: str):
        job, err, code = self._get_job(job_id)
        if job is None:
            return err, code
        kind = request.args.get("kind", "")
        tasks = [self._task_view(t) for t in self.job_manager.tasks_for(job_id, kind)]
        return jsonify({"job_id": job_id, "tasks": tasks})

    def _job_shards(self, job_id: str):
        job, err, code = self._get_job(job_id)
        if job is None:
            return err, code
        return jsonify({
            "job_id": job_id,
            "input_rows": job.input_rows,
            "input_shards": self.job_manager.planner.input_shards(job),
            "map_tasks": [self._task_view(t) for t in self.job_manager.tasks_for(job_id, C.TASK_MAP)],
            "reduce_tasks": [self._task_view(t) for t in self.job_manager.tasks_for(job_id, C.TASK_REDUCE)],
            "shuffle": self.shuffle.matrix(job),
        })

    def _job_shuffle(self, job_id: str):
        job, err, code = self._get_job(job_id)
        if job is None:
            return err, code
        return jsonify(self.shuffle.matrix(job))

    def _job_logs(self, job_id: str):
        job, err, code = self._get_job(job_id)
        if job is None:
            return err, code
        result = self.logbus.query(
            job_id,
            search=request.args.get("q", ""),
            stage=request.args.get("stage", ""),
            task_id=request.args.get("task_id", ""),
            level=request.args.get("level", ""),
            limit=int(request.args.get("limit", 500)),
        )
        result["job_id"] = job_id
        return jsonify(result)

    def _job_metrics(self, job_id: str):
        job, err, code = self._get_job(job_id)
        if job is None:
            return err, code
        data = self.metrics.job_metrics(job_id)
        data["avg_latency_ms"] = round(data["avg_latency_ms"] / 1000.0, 3)
        return jsonify(data)

    def _job_faults(self, job_id: str):
        job, err, code = self._get_job(job_id)
        if job is None:
            return err, code
        faults = self.fault_tolerance.list_faults(job_id)
        faults.reverse()
        return jsonify({"job_id": job_id, "faults": faults})

    def _job_results(self, job_id: str):
        job, err, code = self._get_job(job_id)
        if job is None:
            return err, code
        records = self._read_results(job)
        limit = int(request.args.get("limit", 200))
        return jsonify({
            "job_id": job_id,
            "status": job.status,
            "partitions": self._result_partitions(job),
            "total": len(records),
            "records": records[:limit],
            "truncated": len(records) > limit,
        })

    def _job_results_download(self, job_id: str):
        job, err, code = self._get_job(job_id)
        if job is None:
            return err, code
        records = self._read_results(job)
        fmt = request.args.get("format", "json").lower()
        if fmt == "csv":
            return self._as_csv(job, records)
        body = {"job_id": job_id, "job_name": job.name, "records": records, "count": len(records)}
        return Response(
            io.StringIO(__import__("json").dumps(body, ensure_ascii=False, indent=2)).getvalue(),
            mimetype="application/json",
            headers={"Content-Disposition": f"attachment; filename={job_id}.json"},
        )

    def _as_csv(self, job: Job, records: list[dict]) -> Response:
        fieldnames: list[str] = []
        for rec in records[:50]:
            for key in rec.keys():
                if key != "key" and key not in fieldnames:
                    fieldnames.append(key)
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for rec in records:
            writer.writerow(rec)
        return Response(
            buf.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename={job.job_id}.csv"},
        )

    def _workers(self):
        return jsonify(self.registry.summary())

    def _worker_metrics(self, worker_id: str):
        return jsonify(self.metrics.worker_metrics(worker_id))

    def _cluster_metrics(self):
        return jsonify(self.metrics.cluster_metrics(self.registry.all()))

    def _config(self):
        if request.method == "PUT":
            body = request.get_json(silent=True) or {}
            incoming = ClusterConfig.from_dict(body).validated()
            # Mutate the shared config object in place so the scheduler (which
            # holds the same reference) sees the new values immediately.
            for field in ClusterConfig.__dataclass_fields__:
                if field == "scheduler_tick_sec":
                    continue
                setattr(self.config, field, getattr(incoming, field))
            self.config_manager.save_cluster(self.config)
            return jsonify(self.config.to_dict())
        return jsonify(self.config.to_dict())

    def _config_defaults(self):
        if request.method == "PUT":
            body = request.get_json(silent=True) or {}
            defaults = self.config_manager.save_defaults(JobDefaults.from_dict(body))
            return jsonify(defaults.to_dict())
        return jsonify(self.config_manager.load_defaults().to_dict())

    # ------------------------------------------------------------------
    # Worker-facing routes
    # ------------------------------------------------------------------
    def _worker_register(self):
        body = request.get_json(silent=True) or {}
        if "worker_id" not in body:
            return jsonify({"ok": False, "error": "missing worker_id"}), 400
        worker = self.registry.register(body)
        self.logbus.info("", f"worker {worker.name} registered ({worker.host}:{worker.port})",
                         task_id="cluster", worker_id=worker.worker_id)
        return jsonify({"ok": True, "worker_id": worker.worker_id})

    def _worker_heartbeat(self):
        body = request.get_json(silent=True) or {}
        worker = self.registry.heartbeat(body)
        if worker is None:
            return jsonify({"ok": False, "error": "unknown worker"}), 404
        self.metrics.record_worker(worker)
        return jsonify({"ok": True})

    def _worker_task_status(self):
        body = request.get_json(silent=True) or {}
        self.scheduler.on_task_status(body)
        return jsonify({"ok": True})

    def _worker_task_complete(self):
        body = request.get_json(silent=True) or {}
        self.scheduler.on_task_complete(body)
        return jsonify({"ok": True})

    def _worker_log(self):
        body = request.get_json(silent=True) or {}
        job_id = body.get("job_id", "")
        if not job_id:
            return jsonify({"ok": False}), 400
        self.logbus.emit(
            job_id,
            body.get("level", C.LOG_INFO),
            body.get("message", ""),
            stage=body.get("stage", "master"),
            task_id=body.get("task_id", "job"),
            worker_id=body.get("worker_id", ""),
        )
        return jsonify({"ok": True})
