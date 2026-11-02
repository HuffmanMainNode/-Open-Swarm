from __future__ import annotations

import json
import os
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from openswarm.models import AuditEvent, Task, TaskStatus


class QueueFullError(RuntimeError):
    pass


class TaskNotFoundError(LookupError):
    pass


class InvalidTaskStateError(RuntimeError):
    pass


class LeaseOwnershipError(RuntimeError):
    pass


class TaskQueue:
    def __init__(
        self,
        database: str | Path = ".openswarm/state.sqlite3",
        max_queued: int = 500,
    ) -> None:
        if max_queued < 1:
            raise ValueError("max_queued must be positive")
        self.database = str(database)
        self.max_queued = max_queued
        database_path = Path(self.database)
        database_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if os.name == "posix":
            os.chmod(database_path.parent, 0o700)
        with self._connect() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    lease_id TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_tasks_status_created
                    ON tasks(status, created_at);
                CREATE TABLE IF NOT EXISTS audit (
                    event_id TEXT PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    task_id TEXT,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )
            columns = {row["name"] for row in db.execute("PRAGMA table_info(tasks)")}
            if "lease_id" not in columns:
                db.execute("ALTER TABLE tasks ADD COLUMN lease_id TEXT")
        if os.name == "posix":
            os.chmod(database_path, 0o600)

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.database, timeout=5)
        db.row_factory = sqlite3.Row
        return db

    def _event(self, db: sqlite3.Connection, event: AuditEvent) -> None:
        db.execute(
            "INSERT INTO audit VALUES(?,?,?,?,?)",
            (
                event.event_id,
                event.event_type,
                event.task_id,
                json.dumps(event.detail, sort_keys=True),
                event.created_at,
            ),
        )

    def submit(self, task: Task) -> Task:
        if task.requires_approval:
            task.status = TaskStatus.PENDING_APPROVAL
            task.approved = False
        with self._connect() as db:
            count = db.execute(
                "SELECT COUNT(*) FROM tasks WHERE status IN (?,?)",
                (TaskStatus.QUEUED.value, TaskStatus.PENDING_APPROVAL.value),
            ).fetchone()[0]
            if count >= self.max_queued:
                raise QueueFullError(f"queue capacity {self.max_queued} reached")
            db.execute(
                """
                INSERT INTO tasks(task_id,payload,status,created_at,updated_at,lease_id)
                VALUES(?,?,?,?,?,?)
                """,
                (
                    task.task_id,
                    json.dumps(task.to_dict()),
                    task.status.value,
                    task.created_at,
                    task.updated_at,
                    task.lease_id,
                ),
            )
            self._event(
                db,
                AuditEvent(
                    "task.submitted",
                    task.task_id,
                    {"worker": task.worker, "adapter": task.adapter},
                ),
            )
        return task

    def get(self, task_id: str) -> Task:
        with self._connect() as db:
            row = db.execute(
                "SELECT payload FROM tasks WHERE task_id=?", (task_id,)
            ).fetchone()
        if row is None:
            raise TaskNotFoundError(task_id)
        return Task.from_dict(json.loads(row["payload"]))

    def list(self, status: TaskStatus | None = None, limit: int = 100) -> list[Task]:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        with self._connect() as db:
            if status is None:
                rows = db.execute(
                    "SELECT payload FROM tasks ORDER BY created_at DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            else:
                rows = db.execute(
                    "SELECT payload FROM tasks WHERE status=? ORDER BY created_at DESC LIMIT ?",
                    (status.value, limit),
                ).fetchall()
        return [Task.from_dict(json.loads(row["payload"])) for row in rows]

    def approve(self, task_id: str) -> Task:
        return self._set_approval(task_id, approved=True)

    def reject(self, task_id: str) -> Task:
        return self._set_approval(task_id, approved=False)

    def _set_approval(self, task_id: str, *, approved: bool) -> Task:
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT payload,status FROM tasks WHERE task_id=?", (task_id,)
            ).fetchone()
            if row is None:
                raise TaskNotFoundError(task_id)
            task = Task.from_dict(json.loads(row["payload"]))
            if task.status != TaskStatus.PENDING_APPROVAL:
                raise InvalidTaskStateError("task is not pending approval")
            task.approved = approved
            task.status = TaskStatus.QUEUED if approved else TaskStatus.REJECTED
            task.error = None if approved else "rejected by operator"
            task.updated_at = datetime.now(UTC).isoformat()
            db.execute(
                "UPDATE tasks SET payload=?,status=?,updated_at=? WHERE task_id=?",
                (json.dumps(task.to_dict()), task.status.value, task.updated_at, task_id),
            )
            self._event(
                db,
                AuditEvent(
                    "task.approved" if approved else "task.rejected",
                    task_id,
                    {"operator_action": "approve" if approved else "reject"},
                ),
            )
        return task

    def claim(self, lease_seconds: int = 300) -> Task | None:
        if not 1 <= lease_seconds <= 86400:
            raise ValueError("lease_seconds must be between 1 and 86400")
        now = datetime.now(UTC)
        expired = (now - timedelta(seconds=lease_seconds)).isoformat()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            stale = db.execute(
                "SELECT payload FROM tasks WHERE status=? AND updated_at<?",
                (TaskStatus.RUNNING.value, expired),
            ).fetchall()
            for row in stale:
                old = Task.from_dict(json.loads(row["payload"]))
                old.lease_id = None
                old.updated_at = now.isoformat()
                if old.attempts >= old.max_attempts:
                    old.status = TaskStatus.FAILED
                    old.error = "lease expired after maximum attempts"
                    event_type = "task.lease_exhausted"
                else:
                    old.status = TaskStatus.QUEUED
                    event_type = "task.lease_expired"
                db.execute(
                    "UPDATE tasks SET payload=?,status=?,updated_at=?,lease_id=NULL WHERE task_id=?",
                    (json.dumps(old.to_dict()), old.status.value, old.updated_at, old.task_id),
                )
                self._event(db, AuditEvent(event_type, old.task_id, {"attempts": old.attempts}))

            row = db.execute(
                "SELECT payload FROM tasks WHERE status=? ORDER BY created_at LIMIT 1",
                (TaskStatus.QUEUED.value,),
            ).fetchone()
            if row is None:
                return None
            task = Task.from_dict(json.loads(row["payload"]))
            task.status = TaskStatus.RUNNING
            task.attempts += 1
            task.lease_id = str(uuid4())
            task.updated_at = now.isoformat()
            db.execute(
                """
                UPDATE tasks SET payload=?,status=?,updated_at=?,lease_id=?
                WHERE task_id=? AND status=?
                """,
                (
                    json.dumps(task.to_dict()),
                    task.status.value,
                    task.updated_at,
                    task.lease_id,
                    task.task_id,
                    TaskStatus.QUEUED.value,
                ),
            )
            self._event(
                db,
                AuditEvent("task.claimed", task.task_id, {"attempt": task.attempts}),
            )
            return task

    def finish(
        self,
        task: Task,
        status: TaskStatus,
        result: str | None = None,
        error: str | None = None,
    ) -> Task:
        if status not in {
            TaskStatus.SUCCEEDED,
            TaskStatus.FAILED,
            TaskStatus.BLOCKED,
            TaskStatus.QUEUED,
        }:
            raise ValueError("invalid completion status")
        expected_lease = task.lease_id
        if not expected_lease or task.status != TaskStatus.RUNNING:
            raise LeaseOwnershipError("task has no active lease")
        task.status = status
        task.result = result
        task.error = error
        task.updated_at = datetime.now(UTC).isoformat()
        task.lease_id = None
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            changed = db.execute(
                """
                UPDATE tasks SET payload=?,status=?,updated_at=?,lease_id=NULL
                WHERE task_id=? AND status=? AND lease_id=?
                """,
                (
                    json.dumps(task.to_dict()),
                    status.value,
                    task.updated_at,
                    task.task_id,
                    TaskStatus.RUNNING.value,
                    expected_lease,
                ),
            ).rowcount
            if not changed:
                self._event(
                    db,
                    AuditEvent(
                        "task.finish_rejected",
                        task.task_id,
                        {"reason": "lease_mismatch_or_task_not_running"},
                    ),
                )
                raise LeaseOwnershipError("task lease is no longer owned by this worker")
            self._event(
                db,
                AuditEvent(
                    f"task.{status.value}",
                    task.task_id,
                    {"error": error, "result_chars": len(result or "")},
                ),
            )
        return task

    def audit(self, limit: int = 100) -> list[dict[str, Any]]:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        with self._connect() as db:
            rows = db.execute(
                "SELECT * FROM audit ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [
            {
                "event_id": row["event_id"],
                "event_type": row["event_type"],
                "task_id": row["task_id"],
                "detail": json.loads(row["detail"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]
