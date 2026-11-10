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
        if database_path.parent.is_symlink():
            raise ValueError("queue state directory must not be a symlink")
        database_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if os.name == "posix" and database_path.parent.name == ".openswarm":
            os.chmod(database_path.parent, 0o700)
        if database_path.is_symlink():
            raise ValueError("queue database must not be a symlink")
        if os.name == "posix":
            try:
                descriptor = os.open(
                    self.database, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600
                )
            except FileExistsError:
                os.chmod(database_path, 0o600)
            else:
                os.close(descriptor)
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
                CREATE TABLE IF NOT EXISTS integration_outbox (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    event_type TEXT NOT NULL,
                    task_id TEXT,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    published_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_outbox_pending
                    ON integration_outbox(published_at, sequence);
                """
            )
            columns = {row["name"] for row in db.execute("PRAGMA table_info(tasks)")}
            if "lease_id" not in columns:
                db.execute("ALTER TABLE tasks ADD COLUMN lease_id TEXT")
            queued_rows = db.execute(
                "SELECT task_id,payload FROM tasks WHERE status=?",
                (TaskStatus.QUEUED.value,),
            ).fetchall()
            for row in queued_rows:
                task = Task.from_dict(json.loads(row["payload"]))
                if task.requires_approval and not task.approved:
                    task.status = TaskStatus.PENDING_APPROVAL
                    task.updated_at = datetime.now(UTC).isoformat()
                    db.execute(
                        "UPDATE tasks SET payload=?,status=?,updated_at=? WHERE task_id=?",
                        (
                            json.dumps(task.to_dict()),
                            task.status.value,
                            task.updated_at,
                            task.task_id,
                        ),
                    )
                    self._event(
                        db,
                        AuditEvent(
                            "task.migrated_pending_approval",
                            task.task_id,
                            {"reason": "legacy_unapproved_task"},
                        ),
                    )

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.database, timeout=5)
        db.row_factory = sqlite3.Row
        return db

    def _event(self, db: sqlite3.Connection, event: AuditEvent) -> None:
        detail = json.dumps(event.detail, sort_keys=True)
        db.execute(
            "INSERT INTO audit VALUES(?,?,?,?,?)",
            (
                event.event_id,
                event.event_type,
                event.task_id,
                detail,
                event.created_at,
            ),
        )
        # The outbox write shares the task-state transaction. A relay may
        # publish it later without losing the lifecycle event on process crash.
        db.execute(
            """
            INSERT INTO integration_outbox(
                event_id,event_type,task_id,detail,created_at
            ) VALUES(?,?,?,?,?)
            """,
            (
                event.event_id,
                event.event_type,
                event.task_id,
                detail,
                event.created_at,
            ),
        )

    def pending_outbox(self, limit: int = 100) -> list[dict[str, Any]]:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        with self._connect() as db:
            rows = db.execute(
                """
                SELECT event_id,event_type,task_id,detail,created_at,attempts
                FROM integration_outbox
                WHERE published_at IS NULL
                ORDER BY sequence
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            {
                "event_id": row["event_id"],
                "event_type": row["event_type"],
                "task_id": row["task_id"],
                "detail": json.loads(row["detail"]),
                "created_at": row["created_at"],
                "attempts": row["attempts"],
            }
            for row in rows
        ]

    def record_outbox_attempt(self, event_id: str) -> None:
        with self._connect() as db:
            changed = db.execute(
                """
                UPDATE integration_outbox
                SET attempts=attempts+1
                WHERE event_id=? AND published_at IS NULL
                """,
                (event_id,),
            ).rowcount
            if not changed:
                raise TaskNotFoundError(f"pending outbox event not found: {event_id}")

    def mark_outbox_published(self, event_id: str) -> bool:
        published_at = datetime.now(UTC).isoformat()
        with self._connect() as db:
            row = db.execute(
                "SELECT published_at FROM integration_outbox WHERE event_id=?",
                (event_id,),
            ).fetchone()
            if row is None:
                raise TaskNotFoundError(f"outbox event not found: {event_id}")
            if row["published_at"] is not None:
                return False
            db.execute(
                "UPDATE integration_outbox SET published_at=? WHERE event_id=?",
                (published_at, event_id),
            )
        return True

    def submit(self, task: Task) -> Task:
        if not isinstance(task.max_attempts, int) or isinstance(task.max_attempts, bool):
            raise TypeError("max_attempts must be an integer")
        if task.max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if task.requires_approval:
            task.status = TaskStatus.PENDING_APPROVAL
            task.approved = False
        with self._connect() as db:
            # Serialize capacity check and insertion across independent writers.
            # A plain SELECT does not acquire SQLite's write lock, so two
            # concurrent submitters could otherwise both observe free capacity.
            db.execute("BEGIN IMMEDIATE")
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
                    "UPDATE tasks SET payload=?,status=?,updated_at=?,lease_id=NULL "
                    "WHERE task_id=?",
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

    def oix_sender_is_trusted(self, task_id: str) -> bool | None:
        """Return current trust for an OIX-origin task; None means local task."""
        with self._connect() as db:
            tables = {
                row["name"] for row in db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            if "oix_handoffs" not in tables:
                return None
            origin = db.execute(
                "SELECT sender FROM oix_handoffs WHERE task_id=?", (task_id,)
            ).fetchone()
            if origin is None:
                return None
            if "trusted_peers" not in tables:
                return False
            return db.execute(
                "SELECT 1 FROM trusted_peers WHERE agent_id=?", (origin["sender"],)
            ).fetchone() is not None

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
        task.updated_at = datetime.now(UTC).isoformat()
        task.lease_id = None
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            origin = None
            tables = {
                row["name"] for row in db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            if "oix_handoffs" in tables:
                origin = db.execute(
                    "SELECT sender,message_id FROM oix_handoffs WHERE task_id=?",
                    (task.task_id,),
                ).fetchone()
            origin_trusted = True
            if origin is not None:
                origin_trusted = (
                    "trusted_peers" in tables
                    and db.execute(
                        "SELECT 1 FROM trusted_peers WHERE agent_id=?",
                        (origin["sender"],),
                    ).fetchone() is not None
                )
                if not origin_trusted:
                    status = TaskStatus.BLOCKED
                    result = None
                    error = "OIX sender trust revoked before task completion"
            task.status = status
            task.result = result
            task.error = error
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
                db.commit()
                raise LeaseOwnershipError("task lease is no longer owned by this worker")
            self._event(
                db,
                AuditEvent(
                    f"task.{status.value}",
                    task.task_id,
                    {"error": error, "result_chars": len(result or "")},
                ),
            )
            # If this task originated in OIX, persist a result for operator
            # review in the same transaction as terminal task state and audit.
            # Ordinary local tasks remain unaffected.
            if status in {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.BLOCKED}:
                has_oix_outbox = db.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='oix_result_outbox'"
                ).fetchone()
                if has_oix_outbox:
                    origin = db.execute(
                        "SELECT sender,message_id FROM oix_handoffs WHERE task_id=?",
                        (task.task_id,),
                    ).fetchone()
                    if origin is not None and origin_trusted:
                        result_bytes = (result or "").encode("utf-8")
                        result_omitted = len(result_bytes) > 262_144
                        result_payload = {
                            "task_id": task.task_id,
                            "request_message_id": origin["message_id"],
                            "status": status.value,
                            "result": None if result_omitted else result,
                            "result_omitted": result_omitted,
                        }
                        db.execute(
                            """
                            INSERT OR IGNORE INTO oix_result_outbox(
                                task_id,sender,request_message_id,terminal_status,
                                payload_json,status,created_at
                            ) VALUES(?,?,?,?,?,'pending_approval',?)
                            """,
                            (
                                task.task_id,
                                origin["sender"],
                                origin["message_id"],
                                status.value,
                                json.dumps(
                                    result_payload,
                                    ensure_ascii=False,
                                    sort_keys=True,
                                    separators=(",", ":"),
                                ),
                                task.updated_at,
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
