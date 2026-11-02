from __future__ import annotations

from pathlib import Path

import pytest

from openswarm.events import SQLiteEventBus
from openswarm.models import Task
from openswarm.queue import TaskQueue
from openswarm.relay import OutboxRelay, RelayAlreadyRunning


def test_relay_publishes_outbox_and_reports_status(tmp_path: Path) -> None:
    queue = TaskQueue(tmp_path / "tasks.sqlite3")
    bus = SQLiteEventBus(tmp_path / "events.sqlite3")
    queue.submit(Task(worker="general", prompt="private prompt"))

    relay = OutboxRelay(queue, bus)
    result = relay.run_once()

    assert result.published == 1
    assert result.duplicates == 0
    assert result.pending == 0
    assert bus.count() == 1
    status = relay.status()
    assert status["pending"] == 0
    assert status["published"] == 1
    assert status["attempts"] == 1


def test_relay_recovers_after_publish_succeeds_but_mark_fails(tmp_path: Path) -> None:
    queue = TaskQueue(tmp_path / "tasks.sqlite3")
    bus = SQLiteEventBus(tmp_path / "events.sqlite3")
    queue.submit(Task(worker="general", prompt="not exported"))

    class PublishThenFailBus:
        def publish(self, event):
            bus.publish(event)
            raise OSError("simulated process interruption after durable publish")

    with pytest.raises(OSError):
        OutboxRelay(queue, PublishThenFailBus()).run_once()

    assert queue.pending_outbox()[0]["attempts"] == 1
    result = OutboxRelay(queue, bus).run_once()
    assert result.published == 0
    assert result.duplicates == 1
    assert result.pending == 0
    assert bus.count() == 1


def test_only_one_relay_can_hold_the_database_lease(tmp_path: Path) -> None:
    queue = TaskQueue(tmp_path / "tasks.sqlite3")
    bus = SQLiteEventBus(tmp_path / "events.sqlite3")
    first = OutboxRelay(queue, bus)
    second = OutboxRelay(queue, bus)

    first._acquire()
    try:
        with pytest.raises(RelayAlreadyRunning):
            second.run_once()
    finally:
        first._release()

    assert second.run_once().pending == 0


def test_relay_status_does_not_expose_task_prompt(tmp_path: Path) -> None:
    queue = TaskQueue(tmp_path / "tasks.sqlite3")
    queue.submit(Task(worker="general", prompt="private investigation details"))
    relay = OutboxRelay(queue, SQLiteEventBus(tmp_path / "events.sqlite3"))

    status = relay.status()
    assert "private investigation details" not in str(status)
    assert status["pending"] == 1
    assert status["oldest_pending_age_seconds"] is not None
