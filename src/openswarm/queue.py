import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from openswarm.models import AuditEvent, Task, TaskStatus

class QueueFullError(RuntimeError):
    pass

class TaskNotFoundError(LookupError):
    pass

class TaskQueue:
    def __init__(self, database: str | Path = ".openswarm/state.sqlite3", max_queued: int = 500):
        if max_queued < 1:
            raise ValueError("max_queued must be positive")
        self.database = str(database)
        self.max_queued = max_queued
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY, payload TEXT NOT NULL, status TEXT NOT NULL,
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_tasks_status_created ON tasks(status, created_at);
                CREATE TABLE IF NOT EXISTS audit (
                    event_id TEXT PRIMARY KEY, event_type TEXT NOT NULL, task_id TEXT,
                    detail TEXT NOT NULL, created_at TEXT NOT NULL
                );
            """)

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.database, timeout=5)
        db.row_factory = sqlite3.Row
        return db

    def _event(self, db: sqlite3.Connection, event: AuditEvent) -> None:
        db.execute("INSERT INTO audit VALUES(?,?,?,?,?)",
                   (event.event_id, event.event_type, event.task_id,
                    json.dumps(event.detail, sort_keys=True), event.created_at))

    def submit(self, task: Task) -> Task:
        with self._connect() as db:
            count = db.execute("SELECT COUNT(*) FROM tasks WHERE status=?",
                               (TaskStatus.QUEUED.value,)).fetchone()[0]
            if count >= self.max_queued:
                raise QueueFullError(f"queue capacity {self.max_queued} reached")
            db.execute("INSERT INTO tasks VALUES(?,?,?,?,?)",
                       (task.task_id, json.dumps(task.to_dict()), task.status.value,
                        task.created_at, task.updated_at))
            self._event(db, AuditEvent("task.submitted", task.task_id,
                                       {"worker": task.worker, "adapter": task.adapter}))
        return task

    def get(self, task_id: str) -> Task:
        with self._connect() as db:
            row = db.execute("SELECT payload FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        if row is None:
            raise TaskNotFoundError(task_id)
        return Task.from_dict(json.loads(row["payload"]))

    def list(self, status: TaskStatus | None = None, limit: int = 100) -> list[Task]:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        with self._connect() as db:
            if status is None:
                rows = db.execute("SELECT payload FROM tasks ORDER BY created_at DESC LIMIT ?",
                                  (limit,)).fetchall()
            else:
                rows = db.execute("SELECT payload FROM tasks WHERE status=? ORDER BY created_at LIMIT ?",
                                  (status.value, limit)).fetchall()
        return [Task.from_dict(json.loads(row["payload"])) for row in rows]

    def claim(self, lease_seconds: int = 300) -> Task | None:
        if not 1 <= lease_seconds <= 86400:
            raise ValueError("lease_seconds must be between 1 and 86400")
        now = datetime.now(UTC)
        expired = (now - timedelta(seconds=lease_seconds)).isoformat()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            stale = db.execute("SELECT payload FROM tasks WHERE status=? AND updated_at<?",
                               (TaskStatus.RUNNING.value, expired)).fetchall()
            for row in stale:
                old = Task.from_dict(json.loads(row["payload"]))
                old.status, old.updated_at = TaskStatus.QUEUED, now.isoformat()
                db.execute("UPDATE tasks SET payload=?,status=?,updated_at=? WHERE task_id=?",
                           (json.dumps(old.to_dict()), old.status.value, old.updated_at, old.task_id))
                self._event(db, AuditEvent("task.lease_expired", old.task_id))
            row = db.execute("SELECT payload FROM tasks WHERE status=? ORDER BY created_at LIMIT 1",
                             (TaskStatus.QUEUED.value,)).fetchone()
            if row is None:
                return None
            task = Task.from_dict(json.loads(row["payload"]))
            task.status, task.attempts, task.updated_at = TaskStatus.RUNNING, task.attempts + 1, now.isoformat()
            db.execute("UPDATE tasks SET payload=?,status=?,updated_at=? WHERE task_id=?",
                       (json.dumps(task.to_dict()), task.status.value, task.updated_at, task.task_id))
            self._event(db, AuditEvent("task.claimed", task.task_id, {"attempt": task.attempts}))
            return task

    def finish(self, task: Task, status: TaskStatus, result: str | None = None,
               error: str | None = None) -> Task:
        if status not in {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.BLOCKED, TaskStatus.QUEUED}:
            raise ValueError("invalid completion status")
        task.status, task.result, task.error = status, result, error
        task.updated_at = datetime.now(UTC).isoformat()
        with self._connect() as db:
            changed = db.execute("UPDATE tasks SET payload=?,status=?,updated_at=? WHERE task_id=?",
                                 (json.dumps(task.to_dict()), status.value, task.updated_at,
                                  task.task_id)).rowcount
            if not changed:
                raise TaskNotFoundError(task.task_id)
            self._event(db, AuditEvent(f"task.{status.value}", task.task_id,
                                       {"error": error, "result_chars": len(result or "")}))
        return task

    def audit(self, limit: int = 100) -> list[dict[str, Any]]:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        with self._connect() as db:
            rows = db.execute("SELECT * FROM audit ORDER BY created_at DESC LIMIT ?",
                              (limit,)).fetchall()
        return [{"event_id": r["event_id"], "event_type": r["event_type"],
                 "task_id": r["task_id"], "detail": json.loads(r["detail"]),
                 "created_at": r["created_at"]} for r in rows]
