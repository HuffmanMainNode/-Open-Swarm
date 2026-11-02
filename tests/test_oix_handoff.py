import json
import sqlite3

import pytest

from openswarm.models import Task, TaskStatus
from openswarm.oix_handoff import (
    InvalidRemoteTaskError,
    OIXHandoffError,
    OIXSchemaError,
    OIXTaskHandoff,
)
from openswarm.queue import QueueFullError, TaskQueue


def create_shared_database(path):
    queue = TaskQueue(path)
    with sqlite3.connect(path) as db:
        db.execute(
            """
            CREATE TABLE inbox_messages (
                sender TEXT NOT NULL,
                message_id TEXT NOT NULL,
                nonce TEXT NOT NULL,
                message_type TEXT NOT NULL,
                content_sha256 TEXT NOT NULL,
                body_json TEXT NOT NULL,
                envelope_json TEXT NOT NULL,
                received_at INTEGER NOT NULL,
                PRIMARY KEY(sender,message_id),
                UNIQUE(sender,nonce)
            )
            """
        )
        db.execute(
            """
            CREATE TABLE inbound_work (
                work_id TEXT PRIMARY KEY,
                sender TEXT NOT NULL,
                message_id TEXT NOT NULL,
                work_type TEXT NOT NULL,
                capability TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                decided_at INTEGER,
                UNIQUE(sender,message_id),
                FOREIGN KEY(sender,message_id)
                    REFERENCES inbox_messages(sender,message_id)
            )
            """
        )
    return queue


def add_approved_work(path, *, sender="peer-fingerprint", message_id=None,
                      payload=None, capability="public.research", work_type="task.request",
                      status="approved"):
    import uuid

    message_id = message_id or str(uuid.uuid4())
    work_id = str(uuid.uuid4())
    body = {"type": work_type, "payload": payload or {"prompt": "Summarize public advisories"}}
    with sqlite3.connect(path) as db:
        db.execute(
            """INSERT INTO inbox_messages VALUES(?,?,?,?,?,?,?,?)""",
            (sender, message_id, str(uuid.uuid4()), work_type, "digest", json.dumps(body),
             json.dumps({"body": body, "signature": "signature"}), 1),
        )
        db.execute(
            """INSERT INTO inbound_work VALUES(?,?,?,?,?,?,?,?,?)""",
            (work_id, sender, message_id, work_type, capability, json.dumps(body["payload"]),
             status, 1, 1 if status != "pending_approval" else None),
        )
    return sender, message_id, work_id


def test_approved_oix_task_handoff_is_atomic_and_audited(tmp_path):
    path = tmp_path / "shared.sqlite3"
    queue = create_shared_database(path)
    sender, message_id, work_id = add_approved_work(path)
    handoff = OIXTaskHandoff(queue)

    receipt = handoff.handoff_one(sender, message_id)

    assert receipt.duplicate is False
    assert receipt.work_id == work_id
    task = queue.get(receipt.task_id)
    assert task.prompt == "Summarize public advisories"
    assert task.status == TaskStatus.QUEUED
    assert task.approved is True
    assert task.requires_approval is False
    assert any(event["event_type"] == "task.submitted_remote" for event in queue.audit())
    with sqlite3.connect(path) as db:
        assert db.execute(
            "SELECT status FROM inbound_work WHERE work_id=?", (work_id,)
        ).fetchone()[0] == "dispatched"
        assert db.execute("SELECT COUNT(*) FROM integration_outbox").fetchone()[0] == 1
        assert db.execute("SELECT COUNT(*) FROM oix_handoffs").fetchone()[0] == 1


def test_handoff_retry_returns_same_task_and_never_duplicates(tmp_path):
    path = tmp_path / "shared.sqlite3"
    queue = create_shared_database(path)
    sender, message_id, _ = add_approved_work(path)
    handoff = OIXTaskHandoff(queue)

    first = handoff.handoff_one(sender, message_id)
    second = handoff.handoff_one(sender, message_id)

    assert first.task_id == second.task_id
    assert second.duplicate is True
    assert queue.list(limit=10)[0].task_id == first.task_id
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == 1
        assert db.execute("SELECT COUNT(*) FROM integration_outbox").fetchone()[0] == 1


def test_unapproved_or_wrong_type_work_is_never_handed_off(tmp_path):
    path = tmp_path / "shared.sqlite3"
    queue = create_shared_database(path)
    sender, message_id, _ = add_approved_work(path, status="pending_approval")
    handoff = OIXTaskHandoff(queue)
    with pytest.raises(OIXHandoffError, match="operator-approved"):
        handoff.handoff_one(sender, message_id)

    sender2, message_id2, _ = add_approved_work(
        path, work_type="evidence.submit", payload={"claim": "unverified"}
    )
    with pytest.raises(OIXHandoffError, match="only OIX task.request"):
        handoff.handoff_one(sender2, message_id2)


def test_invalid_payload_leaves_oix_work_approved_and_creates_no_task(tmp_path):
    path = tmp_path / "shared.sqlite3"
    queue = create_shared_database(path)
    sender, message_id, work_id = add_approved_work(
        path, payload={"prompt": "run arbitrary command", "shell": "rm -rf /"}
    )
    handoff = OIXTaskHandoff(queue)
    with pytest.raises(InvalidRemoteTaskError):
        handoff.handoff_one(sender, message_id)
    with sqlite3.connect(path) as db:
        assert db.execute(
            "SELECT status FROM inbound_work WHERE work_id=?", (work_id,)
        ).fetchone()[0] == "approved"
        assert db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM integration_outbox").fetchone()[0] == 0


def test_atomic_failure_rolls_back_task_outbox_mapping_and_inbox_state(tmp_path):
    path = tmp_path / "shared.sqlite3"
    queue = create_shared_database(path)
    sender, message_id, work_id = add_approved_work(path)
    with sqlite3.connect(path) as db:
        db.execute(
            """CREATE TRIGGER fail_oix_handoff BEFORE UPDATE OF status ON inbound_work
               WHEN NEW.status='dispatched'
               BEGIN SELECT RAISE(ABORT,'simulated handoff crash'); END"""
        )
    handoff = OIXTaskHandoff(queue)
    with pytest.raises(sqlite3.IntegrityError):
        handoff.handoff_one(sender, message_id)
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM audit").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM integration_outbox").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM oix_handoffs").fetchone()[0] == 0
        assert db.execute(
            "SELECT status FROM inbound_work WHERE work_id=?", (work_id,)
        ).fetchone()[0] == "approved"


def test_queue_capacity_preserves_approved_inbox_item(tmp_path):
    path = tmp_path / "shared.sqlite3"
    queue = create_shared_database(path)
    queue.max_queued = 1
    queue.submit(Task(worker="general", prompt="occupy queue"))
    sender, message_id, work_id = add_approved_work(path)
    with pytest.raises(QueueFullError):
        OIXTaskHandoff(queue).handoff_one(sender, message_id)
    with sqlite3.connect(path) as db:
        assert db.execute(
            "SELECT status FROM inbound_work WHERE work_id=?", (work_id,)
        ).fetchone()[0] == "approved"


def test_remote_task_payload_bounds_and_capability_binding(tmp_path):
    path = tmp_path / "shared.sqlite3"
    queue = create_shared_database(path)
    sender, message_id, _ = add_approved_work(
        path, payload={"prompt": "inspect", "capabilities": ["filesystem.write"]}
    )
    with pytest.raises(InvalidRemoteTaskError, match="include the OIX-approved"):
        OIXTaskHandoff(queue).handoff_one(sender, message_id)

    sender2, message_id2, _ = add_approved_work(
        path, payload={"prompt": "x" * 16001}
    )
    with pytest.raises(InvalidRemoteTaskError, match="prompt"):
        OIXTaskHandoff(queue).handoff_one(sender2, message_id2)


def test_missing_oix_schema_fails_closed(tmp_path):
    queue = TaskQueue(tmp_path / "ordinary-swarm.sqlite3")
    with pytest.raises(OIXSchemaError):
        OIXTaskHandoff(queue)


def test_handoff_pending_only_moves_approved_task_requests(tmp_path):
    path = tmp_path / "shared.sqlite3"
    queue = create_shared_database(path)
    one = add_approved_work(path)
    add_approved_work(path, status="pending_approval")
    add_approved_work(path, work_type="evidence.submit")
    receipts = OIXTaskHandoff(queue).handoff_pending()
    assert len(receipts) == 1
    assert receipts[0].sender == one[0]
    assert receipts[0].message_id == one[1]



def test_concurrent_handoff_race_creates_exactly_one_task(tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    path = tmp_path / "shared.sqlite3"
    queue = create_shared_database(path)
    sender, message_id, _ = add_approved_work(path)
    handoffs = [OIXTaskHandoff(queue) for _ in range(6)]
    with ThreadPoolExecutor(max_workers=6) as executor:
        receipts = list(
            executor.map(lambda adapter: adapter.handoff_one(sender, message_id), handoffs)
        )
    assert len({receipt.task_id for receipt in receipts}) == 1
    assert sum(not receipt.duplicate for receipt in receipts) == 1
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == 1
        assert db.execute("SELECT COUNT(*) FROM integration_outbox").fetchone()[0] == 1
        assert db.execute("SELECT status FROM inbound_work").fetchone()[0] == "dispatched"
