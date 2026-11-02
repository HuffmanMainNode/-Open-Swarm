from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from openswarm.events import PlatformEvent, SQLiteEventBus
from openswarm.ingestion import (
    IngestionSupervisor,
    SourceBatch,
)


class StaticSource:
    def __init__(self, batch=None, error=None):
        self.batch = batch
        self.error = error
        self.checkpoints = []

    def poll(self, checkpoint):
        self.checkpoints.append(checkpoint)
        if self.error:
            raise self.error
        return self.batch


def event(event_id=None, payload=None):
    return PlatformEvent(
        event_id=event_id or str(uuid4()),
        event_type="intelligence.test.observed",
        source="test-source",
        payload=payload or {"record_count": 1},
        classification="internal",
    )


def setup(tmp_path, source, *, interval=30):
    bus = SQLiteEventBus(tmp_path / "shared.sqlite3")
    supervisor = IngestionSupervisor(bus, lease_seconds=10)
    now = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)
    supervisor.register_source("test.source", source, interval_seconds=interval, now=now)
    return bus, supervisor, now


def test_success_publishes_events_and_advances_checkpoint(tmp_path):
    first, second = event(), event()
    source = StaticSource(SourceBatch([first, second], "cursor-2"))
    bus, supervisor, now = setup(tmp_path, source)

    result = supervisor.run_source_once("test.source", now=now)

    assert result.status == "succeeded"
    assert result.event_count == 2
    assert result.duplicate_count == 0
    assert source.checkpoints == [None]
    status = supervisor.status()[0]
    assert status.checkpoint == "cursor-2"
    assert status.last_success_at is not None
    assert status.consecutive_failures == 0
    with sqlite3.connect(bus.database) as db:
        assert db.execute("SELECT COUNT(*) FROM platform_events").fetchone()[0] == 2


def test_duplicate_event_is_idempotent_across_source_retries(tmp_path):
    item = event()
    source = StaticSource(SourceBatch([item], "cursor-1"))
    bus, supervisor, now = setup(tmp_path, source)

    first = supervisor.run_source_once("test.source", now=now)
    second = supervisor.run_source_once(
        "test.source", force=True, now=now + timedelta(seconds=1)
    )

    assert first.status == "succeeded"
    assert second.status == "succeeded"
    assert second.duplicate_count == 1
    assert supervisor.status()[0].checkpoint == "cursor-1"
    with sqlite3.connect(bus.database) as db:
        assert db.execute("SELECT COUNT(*) FROM platform_events").fetchone()[0] == 1


def test_failed_poll_keeps_checkpoint_and_uses_backoff(tmp_path):
    source = StaticSource(error=TimeoutError("private endpoint details"))
    bus, supervisor, now = setup(tmp_path, source)

    result = supervisor.run_source_once("test.source", now=now)

    assert result.status == "failed"
    assert result.error_type == "TimeoutError"
    status = supervisor.status()[0]
    assert status.checkpoint is None
    assert status.consecutive_failures == 1
    assert status.last_error_type == "TimeoutError"
    assert status.last_success_at is None
    assert status.next_poll_at > now.isoformat().replace("+00:00", "Z")


def test_event_validation_failure_rolls_back_entire_batch_and_checkpoint(tmp_path):
    valid = event()
    invalid = event(payload={"unsafe_float": 1.25})
    source = StaticSource(SourceBatch([valid, invalid], "must-not-advance"))
    bus, supervisor, now = setup(tmp_path, source)

    result = supervisor.run_source_once("test.source", now=now)

    assert result.status == "failed"
    assert result.error_type == "EventValidationError"
    assert supervisor.status()[0].checkpoint is None
    with sqlite3.connect(bus.database) as db:
        assert db.execute("SELECT COUNT(*) FROM platform_events").fetchone()[0] == 0


def test_active_lease_prevents_second_poller(tmp_path):
    source = StaticSource(SourceBatch([], "cursor-1"))
    bus, supervisor, now = setup(tmp_path, source)
    with sqlite3.connect(bus.database) as db:
        db.execute(
            """UPDATE ingestion_sources
               SET lease_id=?,lease_expires_at=? WHERE source_id=?""",
            (
                "other-process",
                (now + timedelta(seconds=30)).isoformat().replace("+00:00", "Z"),
                "test.source",
            ),
        )

    result = supervisor.run_source_once("test.source", now=now)

    assert result.status == "lease_active"
    assert source.checkpoints == []


def test_expired_lease_is_recovered_after_process_crash(tmp_path):
    source = StaticSource(SourceBatch([], "recovered-cursor"))
    bus, supervisor, now = setup(tmp_path, source)
    with sqlite3.connect(bus.database) as db:
        db.execute(
            """UPDATE ingestion_sources
               SET lease_id=?,lease_expires_at=? WHERE source_id=?""",
            (
                "abandoned-process",
                (now - timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
                "test.source",
            ),
        )

    result = supervisor.run_source_once("test.source", now=now)

    assert result.status == "succeeded"
    assert supervisor.status()[0].checkpoint == "recovered-cursor"


def test_not_due_source_is_not_polled(tmp_path):
    source = StaticSource(SourceBatch([], "cursor"))
    _, supervisor, now = setup(tmp_path, source, interval=60)

    first = supervisor.run_source_once("test.source", now=now)
    second = supervisor.run_source_once(
        "test.source", now=now + timedelta(seconds=1)
    )

    assert first.status == "succeeded"
    assert second.status == "not_due"
    assert len(source.checkpoints) == 1


def test_invalid_source_configuration_is_rejected(tmp_path):
    bus = SQLiteEventBus(tmp_path / "events.sqlite3")
    supervisor = IngestionSupervisor(bus)
    source = StaticSource(SourceBatch([], None))
    try:
        supervisor.register_source("../untrusted", source)
    except ValueError as exc:
        assert "source_id" in str(exc)
    else:
        raise AssertionError("unsafe source identifier was accepted")
