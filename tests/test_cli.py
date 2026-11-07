import json
import sys

from openswarm.cli import main
from openswarm.models import TaskStatus
from openswarm.queue import TaskQueue


def _run_cli(monkeypatch, argv):
    monkeypatch.setattr(sys, "argv", ["openswarm", *argv])
    main()


def test_cli_submit_approve_run_tasks_and_audit(tmp_path, monkeypatch, capsys):
    database = str(tmp_path / "cli.sqlite3")
    _run_cli(
        monkeypatch,
        [
            "--db", database, "submit", "--worker", "general",
            "--task", "review", "--requires-approval",
        ],
    )
    task_id = capsys.readouterr().out.strip()
    assert TaskQueue(database).get(task_id).status == TaskStatus.PENDING_APPROVAL

    _run_cli(monkeypatch, ["--db", database, "approve", task_id])
    assert TaskQueue(database).get(task_id).approved

    _run_cli(monkeypatch, ["--db", database, "run-once"])
    assert TaskQueue(database).get(task_id).status == TaskStatus.SUCCEEDED

    _run_cli(monkeypatch, ["--db", database, "tasks"])
    assert task_id in capsys.readouterr().out

    _run_cli(monkeypatch, ["--db", database, "audit"])
    output = capsys.readouterr().out
    assert "task.approved" in output
    assert "task.succeeded" in output


def test_cli_reject_command(tmp_path, monkeypatch, capsys):
    database = str(tmp_path / "cli.sqlite3")
    _run_cli(
        monkeypatch,
        ["--db", database, "submit", "--task", "reject me", "--requires-approval"],
    )
    task_id = capsys.readouterr().out.strip()
    _run_cli(monkeypatch, ["--db", database, "reject", task_id])
    assert TaskQueue(database).get(task_id).status == TaskStatus.REJECTED



def test_cli_demo_completes_with_succeeded_mock_task(tmp_path, monkeypatch, capsys):
    database = str(tmp_path / "demo.sqlite3")
    _run_cli(monkeypatch, ["--db", database, "demo"])
    result = json.loads(capsys.readouterr().out)
    assert result["result"]["status"] == TaskStatus.SUCCEEDED
    assert "[mock response]" in result["result"]["result"]



def test_cli_relay_status_and_single_pass(tmp_path, monkeypatch, capsys):
    database = str(tmp_path / "tasks.sqlite3")
    events_database = str(tmp_path / "events.sqlite3")
    _run_cli(
        monkeypatch,
        ["--db", database, "submit", "--task", "relay me"],
    )
    capsys.readouterr()

    _run_cli(
        monkeypatch,
        ["--db", database, "relay-status", "--events-db", events_database],
    )
    status = json.loads(capsys.readouterr().out)
    assert status["pending"] == 1

    _run_cli(
        monkeypatch,
        ["--db", database, "relay-once", "--events-db", events_database],
    )
    result = json.loads(capsys.readouterr().out)
    assert result == {"published": 1, "duplicates": 0, "pending": 0}


def test_cli_oix_handoff_moves_only_approved_work(tmp_path, monkeypatch, capsys):
    import sqlite3
    import uuid

    database = str(tmp_path / "shared.sqlite3")
    TaskQueue(database)
    sender = "peer-fingerprint"
    message_id = str(uuid.uuid4())
    work_id = str(uuid.uuid4())
    payload = json.dumps({"prompt": "Summarize public advisories"})
    with sqlite3.connect(database) as db:
        db.execute(
            """CREATE TABLE trusted_peers (
                agent_id TEXT PRIMARY KEY,
                signed_card_json TEXT NOT NULL,
                trusted_at INTEGER NOT NULL
            )"""
        )
        db.execute(
            "INSERT INTO trusted_peers VALUES(?,?,?)",
            (sender, "{}", 1),
        )
        db.execute(
            """CREATE TABLE inbox_messages (
                sender TEXT NOT NULL, message_id TEXT NOT NULL, nonce TEXT NOT NULL,
                message_type TEXT NOT NULL, content_sha256 TEXT NOT NULL,
                body_json TEXT NOT NULL, envelope_json TEXT NOT NULL,
                received_at INTEGER NOT NULL, PRIMARY KEY(sender,message_id),
                UNIQUE(sender,nonce)
            )"""
        )
        db.execute(
            """CREATE TABLE inbound_work (
                work_id TEXT PRIMARY KEY, sender TEXT NOT NULL, message_id TEXT NOT NULL,
                work_type TEXT NOT NULL, capability TEXT NOT NULL, payload_json TEXT NOT NULL,
                status TEXT NOT NULL, created_at INTEGER NOT NULL, decided_at INTEGER,
                UNIQUE(sender,message_id)
            )"""
        )
        db.execute(
            "INSERT INTO inbox_messages VALUES(?,?,?,?,?,?,?,?)",
            (sender, message_id, str(uuid.uuid4()), "task.request", "digest", "{}", "{}", 1),
        )
        db.execute(
            "INSERT INTO inbound_work VALUES(?,?,?,?,?,?,?,?,?)",
            (work_id, sender, message_id, "task.request", "public.research", payload,
             "approved", 1, 1),
        )

    _run_cli(monkeypatch, ["--db", database, "oix-handoff"])
    result = json.loads(capsys.readouterr().out)
    assert len(result) == 1
    assert result[0]["task_status"] == TaskStatus.QUEUED.value
    assert TaskQueue(database).get(result[0]["task_id"]).prompt == "Summarize public advisories"
