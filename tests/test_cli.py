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
