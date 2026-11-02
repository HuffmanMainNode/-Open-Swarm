import json
import sqlite3
import uuid

from openswarm.models import TaskStatus
from openswarm.oix_handoff import OIXTaskHandoff
from openswarm.queue import TaskQueue


def _shared_queue(path):
    queue = TaskQueue(path)
    with sqlite3.connect(path) as db:
        db.executescript(
            """
            CREATE TABLE inbox_messages (
                sender TEXT NOT NULL, message_id TEXT NOT NULL, nonce TEXT NOT NULL,
                message_type TEXT NOT NULL, content_sha256 TEXT NOT NULL,
                body_json TEXT NOT NULL, envelope_json TEXT NOT NULL,
                received_at INTEGER NOT NULL, PRIMARY KEY(sender,message_id)
            );
            CREATE TABLE inbound_work (
                work_id TEXT PRIMARY KEY, sender TEXT NOT NULL, message_id TEXT NOT NULL,
                work_type TEXT NOT NULL, capability TEXT NOT NULL, payload_json TEXT NOT NULL,
                status TEXT NOT NULL, created_at INTEGER NOT NULL, decided_at INTEGER,
                UNIQUE(sender,message_id),
                FOREIGN KEY(sender,message_id) REFERENCES inbox_messages(sender,message_id)
            );
            """
        )
        sender = "ed25519:origin-peer"
        message_id = str(uuid.uuid4())
        work_id = str(uuid.uuid4())
        db.execute(
            "INSERT INTO inbox_messages VALUES(?,?,?,?,?,?,?,?)",
            (sender, message_id, "nonce", "task.request", "digest", "{}", "{}", 1),
        )
        db.execute(
            "INSERT INTO inbound_work VALUES(?,?,?,?,?,?,?,?,?)",
            (work_id, sender, message_id, "task.request", "public.research",
             json.dumps({"prompt": "summarize public advisories"}), "approved", 1, 1),
        )
    OIXTaskHandoff(queue)
    return queue, sender, message_id


def _complete_remote_task(queue, sender, message_id, result="public result"):
    receipt = OIXTaskHandoff(queue).handoff_one(sender, message_id)
    claimed = queue.claim()
    assert claimed.task_id == receipt.task_id
    queue.finish(claimed, TaskStatus.SUCCEEDED, result=result)
    return receipt.task_id


def test_terminal_remote_task_creates_pending_approval_result_atomically(tmp_path):
    path = tmp_path / "shared.sqlite3"
    queue, sender, message_id = _shared_queue(path)
    task_id = _complete_remote_task(queue, sender, message_id)

    with sqlite3.connect(path) as db:
        row = db.execute(
            """
            SELECT sender,request_message_id,terminal_status,payload_json,status
            FROM oix_result_outbox WHERE task_id=?
            """,
            (task_id,),
        ).fetchone()
    assert row is not None
    assert row[0] == sender
    assert row[1] == message_id
    assert row[2] == "succeeded"
    assert json.loads(row[3])["result"] == "public result"
    assert row[4] == "pending_approval"
    assert queue.get(task_id).status == TaskStatus.SUCCEEDED


def test_local_task_does_not_create_oix_result(tmp_path):
    path = tmp_path / "shared.sqlite3"
    queue, _, _ = _shared_queue(path)
    local = queue.submit(__import__("openswarm.models", fromlist=["Task"]).Task(
        worker="general", prompt="local-only"
    ))
    claimed = queue.claim()
    assert claimed.task_id == local.task_id
    queue.finish(claimed, TaskStatus.SUCCEEDED, result="local result")
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT COUNT(*) FROM oix_result_outbox").fetchone()[0] == 0


def test_oversized_result_is_not_silently_truncated_for_release(tmp_path):
    path = tmp_path / "shared.sqlite3"
    queue, sender, message_id = _shared_queue(path)
    task_id = _complete_remote_task(queue, sender, message_id, result="x" * 262145)
    with sqlite3.connect(path) as db:
        payload = json.loads(db.execute(
            "SELECT payload_json FROM oix_result_outbox WHERE task_id=?", (task_id,)
        ).fetchone()[0])
    assert payload["result"] is None
    assert payload["result_omitted"] is True
    assert payload["status"] == "succeeded"


def test_failed_or_blocked_remote_task_also_waits_for_approval(tmp_path):
    path = tmp_path / "shared.sqlite3"
    queue, sender, message_id = _shared_queue(path)
    receipt = OIXTaskHandoff(queue).handoff_one(sender, message_id)
    claimed = queue.claim()
    queue.finish(claimed, TaskStatus.BLOCKED, error="policy blocked")
    with sqlite3.connect(path) as db:
        row = db.execute(
            "SELECT terminal_status,status FROM oix_result_outbox WHERE task_id=?",
            (receipt.task_id,),
        ).fetchone()
    assert row == ("blocked", "pending_approval")
