"""Atomic handoff from an approved OIX SQLite inbox into the Open-Swarm queue.

This module intentionally has no dependency on the OIX package. It consumes the
documented SQLite inbox contract, allowing both projects to remain independently
installable. The OIX inbox and Open-Swarm queue MUST point at the same SQLite file.
"""
from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from openswarm.models import AuditEvent, Task, TaskStatus
from openswarm.queue import QueueFullError, TaskQueue

_ALLOWED_PAYLOAD_FIELDS = {
    "worker", "prompt", "adapter", "model", "capabilities", "max_attempts"
}
_WORKER_NAME = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")
_MAX_PROMPT_CHARS = 16_000
_MAX_CAPABILITIES = 32


class OIXHandoffError(RuntimeError):
    """An approved OIX work item cannot safely become an Open-Swarm task."""


class OIXSchemaError(OIXHandoffError):
    """The configured database does not contain the expected OIX inbox schema."""


class InvalidRemoteTaskError(OIXHandoffError):
    """An approved remote task has an invalid or unsupported payload."""


@dataclass(frozen=True, slots=True)
class HandoffReceipt:
    sender: str
    message_id: str
    work_id: str
    task_id: str
    duplicate: bool
    task_status: str


class OIXTaskHandoff:
    """Move operator-approved OIX task requests into Open-Swarm atomically.

    OIX must first verify the signature, trusted sender, recipient, expiry,
    nonce, and local capability policy. An operator must then approve the
    resulting OIX inbound_work row. This adapter never accepts raw envelopes.

    For each handoff, one BEGIN IMMEDIATE transaction:
      * checks the approved OIX inbox row and stable remote message key;
      * inserts the Open-Swarm task, audit event, and lifecycle outbox row;
      * records the OIX-to-task mapping; and
      * changes OIX inbound_work from approved to dispatched.

    All writes use the same SQLite database connection. A rollback leaves the
    OIX item approved and creates no task or lifecycle event.
    """

    def __init__(self, queue: TaskQueue) -> None:
        if not isinstance(queue, TaskQueue):
            raise TypeError("queue must be an Open-Swarm TaskQueue")
        self.queue = queue
        self.database = queue.database
        with self.queue._connect() as db:
            db.execute("PRAGMA foreign_keys=ON")
            self._require_oix_schema(db)
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS oix_handoffs (
                    sender TEXT NOT NULL,
                    message_id TEXT NOT NULL,
                    work_id TEXT NOT NULL UNIQUE,
                    task_id TEXT NOT NULL UNIQUE,
                    handed_off_at TEXT NOT NULL,
                    PRIMARY KEY(sender, message_id),
                    FOREIGN KEY(sender, message_id)
                        REFERENCES inbox_messages(sender, message_id),
                    FOREIGN KEY(work_id)
                        REFERENCES inbound_work(work_id),
                    FOREIGN KEY(task_id)
                        REFERENCES tasks(task_id)
                )
                """
            )

    @staticmethod
    def _require_oix_schema(db: sqlite3.Connection) -> None:
        tables = {
            row["name"]
            for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        required = {"inbox_messages", "inbound_work"}
        if not required.issubset(tables):
            missing = ", ".join(sorted(required - tables))
            raise OIXSchemaError(f"missing OIX inbox tables: {missing}")
        inbox_columns = {
            row["name"] for row in db.execute("PRAGMA table_info(inbox_messages)")
        }
        if not {"sender", "message_id"}.issubset(inbox_columns):
            raise OIXSchemaError("OIX inbox_messages schema is incompatible")
        columns = {
            row["name"] for row in db.execute("PRAGMA table_info(inbound_work)")
        }
        required_columns = {
            "work_id", "sender", "message_id", "work_type", "capability",
            "payload_json", "status", "created_at"
        }
        if not required_columns.issubset(columns):
            raise OIXSchemaError("OIX inbound_work schema is incompatible")

    @staticmethod
    def _build_task(row: sqlite3.Row) -> Task:
        try:
            payload: Any = json.loads(row["payload_json"])
        except (TypeError, ValueError) as exc:
            raise InvalidRemoteTaskError("approved OIX task payload is not valid JSON") from exc
        if not isinstance(payload, dict):
            raise InvalidRemoteTaskError("approved OIX task payload must be an object")
        unknown = set(payload) - _ALLOWED_PAYLOAD_FIELDS
        if unknown:
            raise InvalidRemoteTaskError("approved OIX task contains unsupported fields")
        prompt = payload.get("prompt")
        if not isinstance(prompt, str) or not prompt.strip() or len(prompt) > _MAX_PROMPT_CHARS:
            raise InvalidRemoteTaskError("prompt must contain 1 to 16000 characters")
        if "\x00" in prompt:
            raise InvalidRemoteTaskError("prompt must not contain NUL characters")

        worker = payload.get("worker", "general")
        if not isinstance(worker, str) or not _WORKER_NAME.fullmatch(worker):
            raise InvalidRemoteTaskError("worker must be a 1 to 64 character identifier")
        adapter = payload.get("adapter", "mock")
        if not isinstance(adapter, str) or not 1 <= len(adapter) <= 64:
            raise InvalidRemoteTaskError("adapter must contain 1 to 64 characters")
        model = payload.get("model")
        if model is not None and (
            not isinstance(model, str) or not 1 <= len(model) <= 128
        ):
            raise InvalidRemoteTaskError("model must be null or contain 1 to 128 characters")

        capabilities = payload.get("capabilities", [row["capability"]])
        if (
            not isinstance(capabilities, list)
            or not 1 <= len(capabilities) <= _MAX_CAPABILITIES
            or any(
                not isinstance(item, str) or not 1 <= len(item) <= 128
                for item in capabilities
            )
            or len(set(capabilities)) != len(capabilities)
        ):
            raise InvalidRemoteTaskError("capabilities must be 1 to 32 unique labels")
        if capabilities != [row["capability"]]:
            raise InvalidRemoteTaskError(
                "task capabilities must contain only the OIX-approved requested capability"
            )

        max_attempts = payload.get("max_attempts", 2)
        if (
            not isinstance(max_attempts, int)
            or isinstance(max_attempts, bool)
            or not 1 <= max_attempts <= 10
        ):
            raise InvalidRemoteTaskError("max_attempts must be an integer from 1 to 10")

        stable_task_id = str(
            uuid5(NAMESPACE_URL, f"oix:{row['sender']}:{row['message_id']}")
        )
        now = datetime.now(UTC).isoformat()
        return Task(
            worker=worker,
            prompt=prompt,
            adapter=adapter,
            model=model,
            capabilities=capabilities,
            requires_approval=False,
            approved=True,
            max_attempts=max_attempts,
            task_id=stable_task_id,
            status=TaskStatus.QUEUED,
            created_at=now,
            updated_at=now,
        )

    def handoff_one(self, sender: str, message_id: str) -> HandoffReceipt:
        """Atomically hand off one previously approved OIX task request."""
        if not isinstance(sender, str) or not sender:
            raise ValueError("sender must be a non-empty string")
        if not isinstance(message_id, str) or not message_id:
            raise ValueError("message_id must be a non-empty string")

        with self.queue._connect() as db:
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("BEGIN IMMEDIATE")
            self._require_oix_schema(db)
            existing = db.execute(
                """
                SELECT h.work_id,h.task_id,t.status
                FROM oix_handoffs AS h
                JOIN tasks AS t ON t.task_id=h.task_id
                WHERE h.sender=? AND h.message_id=?
                """,
                (sender, message_id),
            ).fetchone()
            if existing is not None:
                db.commit()
                return HandoffReceipt(
                    sender, message_id, existing["work_id"], existing["task_id"],
                    True, existing["status"]
                )

            row = db.execute(
                """
                SELECT work_id,sender,message_id,work_type,capability,payload_json,
                       status,created_at
                FROM inbound_work
                WHERE sender=? AND message_id=?
                """,
                (sender, message_id),
            ).fetchone()
            if row is None:
                raise OIXHandoffError("OIX inbound work item does not exist")
            if row["work_type"] != "task.request":
                raise OIXHandoffError("only OIX task.request items can enter the task queue")
            if row["status"] != "approved":
                raise OIXHandoffError("OIX task must be operator-approved before handoff")

            task = self._build_task(row)
            count = db.execute(
                "SELECT COUNT(*) FROM tasks WHERE status IN (?,?)",
                (TaskStatus.QUEUED.value, TaskStatus.PENDING_APPROVAL.value),
            ).fetchone()[0]
            if count >= self.queue.max_queued:
                raise QueueFullError(f"queue capacity {self.queue.max_queued} reached")

            db.execute(
                """
                INSERT INTO tasks(task_id,payload,status,created_at,updated_at,lease_id)
                VALUES(?,?,?,?,?,?)
                """,
                (
                    task.task_id, json.dumps(task.to_dict()), task.status.value,
                    task.created_at, task.updated_at, task.lease_id,
                ),
            )
            detail = {
                "origin": "oix",
                "sender": sender,
                "message_id": message_id,
                "work_id": row["work_id"],
                "capability": row["capability"],
            }
            self.queue._event(
                db,
                AuditEvent("task.submitted_remote", task.task_id, detail),
            )
            db.execute(
                """
                INSERT INTO oix_handoffs(sender,message_id,work_id,task_id,handed_off_at)
                VALUES(?,?,?,?,?)
                """,
                (
                    sender, message_id, row["work_id"], task.task_id,
                    datetime.now(UTC).isoformat(),
                ),
            )
            changed = db.execute(
                """
                UPDATE inbound_work SET status='dispatched'
                WHERE work_id=? AND status='approved'
                """,
                (row["work_id"],),
            ).rowcount
            if changed != 1:
                raise OIXHandoffError("OIX approval changed during task handoff")
            db.commit()

        return HandoffReceipt(
            sender, message_id, row["work_id"], task.task_id, False, task.status.value
        )

    def handoff_pending(self, *, limit: int = 100) -> list[HandoffReceipt]:
        """Hand off approved remote task requests in stable creation order."""
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        with self.queue._connect() as db:
            self._require_oix_schema(db)
            rows = db.execute(
                """
                SELECT sender,message_id FROM inbound_work
                WHERE work_type='task.request' AND status='approved'
                ORDER BY created_at,work_id LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            self.handoff_one(row["sender"], row["message_id"])
            for row in rows
        ]
