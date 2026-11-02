from __future__ import annotations

from pathlib import Path

import pytest

from openswarm.events import (
    ConsumerError,
    EventConflictError,
    EventValidationError,
    PlatformEvent,
    SQLiteEventBus,
    publish_outbox,
)
from openswarm.models import Task, TaskStatus
from openswarm.queue import TaskQueue


def make_event(event_id: str = "8b6a4f8d-9a3b-4e28-8d8e-0e5d9f7d1b11") -> PlatformEvent:
    return PlatformEvent(
        event_id=event_id,
        event_type="osint.indicator.observed",
        source="halloween-osint",
        classification="public",
        payload={"indicator_type": "domain", "indicator": "example.org"},
        evidence_refs=["sha256:" + "a" * 64],
    )


def test_publish_read_ack_and_persistence(tmp_path: Path) -> None:
    database = tmp_path / "events.sqlite3"
    bus = SQLiteEventBus(database)
    receipt = bus.publish(make_event())
    assert receipt.sequence == 1
    assert receipt.duplicate is False

    records = bus.read("observatory", limit=10)
    assert len(records) == 1
    assert records[0].event.payload["indicator"] == "example.org"
    assert bus.consumer_offset("observatory") == 0

    bus.acknowledge("observatory", records[-1].sequence)
    assert bus.consumer_offset("observatory") == 1
    assert SQLiteEventBus(database).read("observatory") == []


def test_independent_consumers_receive_same_event(tmp_path: Path) -> None:
    bus = SQLiteEventBus(tmp_path / "events.sqlite3")
    bus.publish(make_event())
    assert len(bus.read("observatory")) == 1
    assert len(bus.read("swarm")) == 1


def test_publish_is_idempotent_for_identical_event(tmp_path: Path) -> None:
    bus = SQLiteEventBus(tmp_path / "events.sqlite3")
    event = make_event()
    first = bus.publish(event)
    second = bus.publish(event)
    assert first.sequence == second.sequence
    assert second.duplicate is True
    assert bus.count() == 1


def test_reusing_event_id_with_different_content_is_rejected(tmp_path: Path) -> None:
    bus = SQLiteEventBus(tmp_path / "events.sqlite3")
    bus.publish(make_event())
    conflicting = make_event()
    conflicting.payload = {"indicator_type": "domain", "indicator": "other.example"}
    with pytest.raises(EventConflictError):
        bus.publish(conflicting)


def test_cannot_acknowledge_unread_or_undelivered_sequence(tmp_path: Path) -> None:
    bus = SQLiteEventBus(tmp_path / "events.sqlite3")
    bus.publish(make_event())
    with pytest.raises(ConsumerError):
        bus.acknowledge("unknown", 1)
    bus.read("consumer", limit=1)
    with pytest.raises(ConsumerError):
        bus.acknowledge("consumer", 2)


def test_validation_rejects_bad_classification_and_naive_time(tmp_path: Path) -> None:
    bus = SQLiteEventBus(tmp_path / "events.sqlite3")
    event = make_event()
    event.classification = "secret"
    with pytest.raises(EventValidationError):
        bus.publish(event)
    event = make_event("c5cfa6f7-c15e-4f4b-a0cf-8e73ed90dbd2")
    event.occurred_at = "2026-11-01T12:00:00"
    with pytest.raises(EventValidationError):
        bus.publish(event)


def test_validation_rejects_non_json_and_oversized_payload(tmp_path: Path) -> None:
    bus = SQLiteEventBus(tmp_path / "events.sqlite3")
    event = make_event()
    event.payload = {"invalid": object()}
    with pytest.raises(EventValidationError):
        bus.publish(event)
    event = make_event("ab0ef02c-2f0b-4a2d-9f8e-1b2c3d4e5f60")
    event.payload = {"data": "x" * 70_000}
    with pytest.raises(EventValidationError):
        bus.publish(event)


def test_consumer_must_ack_after_processing_to_advance_offset(tmp_path: Path) -> None:
    bus = SQLiteEventBus(tmp_path / "events.sqlite3")
    bus.publish(make_event())
    first = bus.read("consumer")
    assert len(first) == 1
    assert bus.read("consumer") == [first[0]]
    bus.acknowledge("consumer", first[0].sequence)
    assert bus.read("consumer") == []



def test_validation_rejects_floats_and_non_string_object_keys(tmp_path: Path) -> None:
    bus = SQLiteEventBus(tmp_path / "events.sqlite3")
    event = make_event()
    event.payload = {"score": 0.75}
    with pytest.raises(EventValidationError):
        bus.publish(event)
    event = make_event("4aebd8e6-c451-40c6-9c93-14e6c0a09a25")
    event.payload = {1: "not a valid JSON object key"}
    with pytest.raises(EventValidationError):
        bus.publish(event)


def test_validation_rejects_excessive_nesting(tmp_path: Path) -> None:
    bus = SQLiteEventBus(tmp_path / "events.sqlite3")
    event = make_event()
    value: object = "leaf"
    for _ in range(34):
        value = {"nested": value}
    event.payload = {"data": value}
    with pytest.raises(EventValidationError):
        bus.publish(event)



def test_task_lifecycle_uses_transactional_outbox(tmp_path: Path) -> None:
    queue = TaskQueue(tmp_path / "tasks.sqlite3")
    bus = SQLiteEventBus(tmp_path / "events.sqlite3")
    queue.submit(Task(worker="general", prompt="private investigation prompt"))
    pending = queue.pending_outbox()
    assert len(pending) == 1
    assert pending[0]["event_type"] == "task.submitted"
    assert "private investigation prompt" not in str(pending[0]["detail"])

    receipts = publish_outbox(queue, bus)
    assert len(receipts) == 1
    assert receipts[0].duplicate is False
    assert queue.pending_outbox() == []
    received = bus.read("observatory")
    assert len(received) == 1
    assert received[0].event.event_id == pending[0]["event_id"]
    assert received[0].event.payload["task_id"] is not None


def test_outbox_relay_is_safe_after_publish_before_mark(tmp_path: Path) -> None:
    queue = TaskQueue(tmp_path / "tasks.sqlite3")
    bus = SQLiteEventBus(tmp_path / "events.sqlite3")
    queue.submit(Task(worker="general", prompt="not exported"))
    record = queue.pending_outbox()[0]
    bus.publish(
        PlatformEvent(
            event_id=record["event_id"],
            event_type=record["event_type"],
            source="open-swarm",
            classification="internal",
            correlation_id=record["task_id"],
            occurred_at=record["created_at"],
            payload={"task_id": record["task_id"], "detail": record["detail"]},
        )
    )
    receipts = publish_outbox(queue, bus)
    assert len(receipts) == 1
    assert receipts[0].duplicate is True
    assert bus.count() == 1
    assert queue.pending_outbox() == []



def test_outbox_captures_task_approval_and_completion(tmp_path: Path) -> None:
    queue = TaskQueue(tmp_path / "tasks.sqlite3")
    task = Task(
        worker="general",
        prompt="sensitive task body",
        requires_approval=True,
    )
    queue.submit(task)
    assert [item["event_type"] for item in queue.pending_outbox()] == [
        "task.submitted"
    ]

    queue.approve(task.task_id)
    claimed = queue.claim()
    assert claimed is not None
    queue.finish(claimed, TaskStatus.SUCCEEDED, result="private result")

    event_types = [item["event_type"] for item in queue.pending_outbox()]
    assert event_types == [
        "task.submitted",
        "task.approved",
        "task.claimed",
        "task.succeeded",
    ]
    serialized = str(queue.pending_outbox())
    assert "sensitive task body" not in serialized
    assert "private result" not in serialized


def test_outbox_attempt_count_increases_before_delivery(tmp_path: Path) -> None:
    queue = TaskQueue(tmp_path / "tasks.sqlite3")
    queue.submit(Task(worker="general", prompt="task"))
    record = queue.pending_outbox()[0]
    queue.record_outbox_attempt(record["event_id"])
    assert queue.pending_outbox()[0]["attempts"] == 1
    assert queue.mark_outbox_published(record["event_id"]) is True
    assert queue.mark_outbox_published(record["event_id"]) is False
