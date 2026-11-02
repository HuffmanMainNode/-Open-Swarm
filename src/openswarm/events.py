"""Durable, local-first event journal for Open-Swarm integrations.

This is a single-host SQLite event log with explicit consumer acknowledgements.
It is not a network broker and does not claim exactly-once delivery.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

MAX_EVENT_BYTES = 65_536
MAX_EVIDENCE_REFS = 100
EVENT_TYPE = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)+$")
CLASSIFICATIONS = {"public", "internal", "restricted"}


class EventValidationError(ValueError):
    """An event violates the shared event contract."""


class EventConflictError(RuntimeError):
    """An event ID was reused for different event content."""


class ConsumerError(RuntimeError):
    """A consumer attempted an invalid offset operation."""


@dataclass(slots=True)
class PlatformEvent:
    event_type: str
    source: str
    payload: dict[str, Any]
    schema_version: str = "1.0"
    classification: str = "internal"
    evidence_refs: list[str] = field(default_factory=list)
    correlation_id: str | None = None
    causation_id: str | None = None
    event_id: str = field(default_factory=lambda: str(uuid4()))
    occurred_at: str = field(
        default_factory=lambda: datetime.now(UTC).isoformat().replace("+00:00", "Z")
    )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class EventReceipt:
    event_id: str
    sequence: int
    duplicate: bool


@dataclass(frozen=True, slots=True)
class EventRecord:
    sequence: int
    event: PlatformEvent


def _validate_json_value(value: Any, *, depth: int = 0) -> None:
    """Accept only deterministic JSON values used by the cross-language contract."""
    if depth > 32:
        raise EventValidationError("payload nesting exceeds 32 levels")
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        raise EventValidationError(
            "floating-point values are prohibited; use integers or decimal strings"
        )
    if isinstance(value, list):
        if len(value) > 10_000:
            raise EventValidationError("payload arrays may contain at most 10000 items")
        for item in value:
            _validate_json_value(item, depth=depth + 1)
        return
    if isinstance(value, dict):
        if len(value) > 10_000:
            raise EventValidationError("payload objects may contain at most 10000 members")
        for key, item in value.items():
            if not isinstance(key, str):
                raise EventValidationError("payload object keys must be strings")
            _validate_json_value(item, depth=depth + 1)
        return
    raise EventValidationError("payload contains a value outside the JSON data model")


def _validate_event(event: PlatformEvent) -> dict[str, str]:
    if not isinstance(event, PlatformEvent):
        raise EventValidationError("event must be a PlatformEvent")
    if not isinstance(event.event_id, str):
        raise EventValidationError("event_id must be a canonical lowercase UUID")
    try:
        if str(UUID(event.event_id)) != event.event_id:
            raise ValueError("UUID must use canonical lowercase form")
    except (ValueError, TypeError, AttributeError) as exc:
        raise EventValidationError("event_id must be a canonical lowercase UUID") from exc
    if (
        not isinstance(event.event_type, str)
        or len(event.event_type) > 128
        or not EVENT_TYPE.fullmatch(event.event_type)
    ):
        raise EventValidationError("event_type must be a namespaced lowercase identifier")
    if not isinstance(event.source, str) or not event.source.strip() or len(event.source) > 128:
        raise EventValidationError("source must contain 1 to 128 characters")
    if (
        not isinstance(event.schema_version, str)
        or len(event.schema_version) > 32
        or not re.fullmatch(r"[0-9]+\.[0-9]+", event.schema_version)
    ):
        raise EventValidationError("schema_version must use MAJOR.MINOR notation")
    if (
        not isinstance(event.classification, str)
        or event.classification not in CLASSIFICATIONS
    ):
        raise EventValidationError("classification must be public, internal, or restricted")
    if not isinstance(event.payload, dict):
        raise EventValidationError("payload must be a JSON object")
    _validate_json_value(event.payload)
    if not isinstance(event.evidence_refs, list) or len(event.evidence_refs) > MAX_EVIDENCE_REFS:
        raise EventValidationError("evidence_refs must be a list of at most 100 references")
    if any(
        not isinstance(ref, str) or not ref.strip() or len(ref) > 512
        for ref in event.evidence_refs
    ):
        raise EventValidationError("each evidence reference must contain 1 to 512 characters")
    for label, value in (
        ("correlation_id", event.correlation_id),
        ("causation_id", event.causation_id),
    ):
        if value is not None and (
            not isinstance(value, str) or not value.strip() or len(value) > 128
        ):
            raise EventValidationError(f"{label} must contain 1 to 128 characters when supplied")
    if not isinstance(event.occurred_at, str):
        raise EventValidationError("occurred_at must be an RFC 3339 timestamp")
    try:
        timestamp = datetime.fromisoformat(event.occurred_at.replace("Z", "+00:00"))
        if timestamp.tzinfo is None:
            raise ValueError("timezone required")
    except ValueError as exc:
        raise EventValidationError(
            "occurred_at must be an RFC 3339 timestamp with timezone"
        ) from exc
    try:
        payload_json = json.dumps(
            event.payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        evidence_json = json.dumps(
            event.evidence_refs, ensure_ascii=False, separators=(",", ":")
        )
        encoded = json.dumps(
            event.to_dict(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, RecursionError) as exc:
        raise EventValidationError(
            "payload and evidence references must be finite JSON data"
        ) from exc
    if len(encoded) > MAX_EVENT_BYTES:
        raise EventValidationError(f"event exceeds {MAX_EVENT_BYTES} bytes")
    return {
        "payload": payload_json,
        "evidence_refs": evidence_json,
        "canonical": encoded.decode("utf-8"),
    }


class SQLiteEventBus:
    """Append-only event journal with per-consumer at-least-once delivery.

    A consumer must process a batch before acknowledging its final sequence.
    Use one active poller per consumer ID. Multiple independent consumers may
    read the same event stream with independent offsets.
    """

    def __init__(
        self,
        database: str | Path = ".openswarm/events.sqlite3",
        *,
        busy_timeout_ms: int = 5000,
    ) -> None:
        if not 1 <= busy_timeout_ms <= 60_000:
            raise ValueError("busy_timeout_ms must be between 1 and 60000")
        self.database = str(database)
        self.busy_timeout_ms = busy_timeout_ms
        path = Path(self.database)
        if path.parent.is_symlink():
            raise ValueError("event journal directory must not be a symlink")
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if os.name == "posix" and path.parent.name == ".openswarm":
            os.chmod(path.parent, 0o700)
        if path.is_symlink():
            raise ValueError("event journal database must not be a symlink")
        if os.name == "posix":
            try:
                descriptor = os.open(
                    self.database, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600
                )
            except FileExistsError:
                os.chmod(path, 0o600)
            else:
                os.close(descriptor)
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS platform_events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    event_type TEXT NOT NULL,
                    schema_version TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    source TEXT NOT NULL,
                    classification TEXT NOT NULL,
                    correlation_id TEXT,
                    causation_id TEXT,
                    evidence_refs_json TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    canonical_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_platform_events_type_sequence
                    ON platform_events(event_type, sequence);
                CREATE TABLE IF NOT EXISTS event_consumers (
                    consumer_id TEXT PRIMARY KEY,
                    last_sequence INTEGER NOT NULL DEFAULT 0,
                    delivered_sequence INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.database, timeout=self.busy_timeout_ms / 1000)
        db.row_factory = sqlite3.Row
        db.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        return db

    def publish_in_transaction(
        self, connection: sqlite3.Connection, event: PlatformEvent
    ) -> EventReceipt:
        """Append an event inside a caller-owned transaction on this same database.

        This enables adapters to atomically persist a domain event and advance
        the source system's durable state without a dual-write gap.
        """
        if not isinstance(connection, sqlite3.Connection) or not connection.in_transaction:
            raise ValueError("connection must be an active SQLite transaction")
        database_rows = connection.execute("PRAGMA database_list").fetchall()
        main_path = next((row[2] for row in database_rows if row[1] == "main"), "")
        if not main_path or Path(main_path).resolve() != Path(self.database).resolve():
            raise ValueError("transaction connection must target the event journal database")
        encoded = _validate_event(event)
        try:
            cursor = connection.execute(
                """
                INSERT INTO platform_events(
                    event_id,event_type,schema_version,occurred_at,source,classification,
                    correlation_id,causation_id,evidence_refs_json,payload_json,canonical_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    event.event_id,
                    event.event_type,
                    event.schema_version,
                    event.occurred_at,
                    event.source,
                    event.classification,
                    event.correlation_id,
                    event.causation_id,
                    encoded["evidence_refs"],
                    encoded["payload"],
                    encoded["canonical"],
                ),
            )
            return EventReceipt(event.event_id, int(cursor.lastrowid), False)
        except sqlite3.IntegrityError as exc:
            existing = connection.execute(
                "SELECT sequence,canonical_json FROM platform_events WHERE event_id=?",
                (event.event_id,),
            ).fetchone()
            if existing is None:
                raise
            if existing["canonical_json"] != encoded["canonical"]:
                raise EventConflictError(
                    "event_id already exists with different content"
                ) from exc
            return EventReceipt(event.event_id, int(existing["sequence"]), True)

    def publish(self, event: PlatformEvent) -> EventReceipt:
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            return self.publish_in_transaction(db, event)

    def read(self, consumer_id: str, *, limit: int = 100) -> list[EventRecord]:
        if not isinstance(consumer_id, str) or not consumer_id.strip() or len(consumer_id) > 128:
            raise ConsumerError("consumer_id must contain 1 to 128 characters")
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        with self._connect() as db:
            db.execute(
                """
                INSERT OR IGNORE INTO event_consumers(
                    consumer_id,last_sequence,delivered_sequence,created_at,updated_at
                ) VALUES(?,0,0,?,?)
                """,
                (consumer_id, now, now),
            )
            consumer = db.execute(
                "SELECT last_sequence FROM event_consumers WHERE consumer_id=?",
                (consumer_id,),
            ).fetchone()
            rows = db.execute(
                "SELECT * FROM platform_events WHERE sequence>? ORDER BY sequence LIMIT ?",
                (consumer["last_sequence"], limit),
            ).fetchall()
            if rows:
                db.execute(
                    """
                    UPDATE event_consumers SET delivered_sequence=?,updated_at=?
                    WHERE consumer_id=?
                    """,
                    (int(rows[-1]["sequence"]), now, consumer_id),
                )
        return [self._record(row) for row in rows]

    def acknowledge(self, consumer_id: str, sequence: int) -> None:
        if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 1:
            raise ConsumerError("sequence must be a positive integer")
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            consumer = db.execute(
                "SELECT last_sequence,delivered_sequence FROM event_consumers WHERE consumer_id=?",
                (consumer_id,),
            ).fetchone()
            if consumer is None:
                raise ConsumerError("consumer has not read from the event stream")
            if sequence == consumer["last_sequence"]:
                return
            if sequence < consumer["last_sequence"] or sequence > consumer["delivered_sequence"]:
                raise ConsumerError("cannot acknowledge an event that was not delivered")
            exists = db.execute(
                "SELECT 1 FROM platform_events WHERE sequence=?", (sequence,)
            ).fetchone()
            if exists is None:
                raise ConsumerError("event sequence does not exist")
            db.execute(
                "UPDATE event_consumers SET last_sequence=?,updated_at=? WHERE consumer_id=?",
                (
                    sequence,
                    datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                    consumer_id,
                ),
            )

    def consumer_offset(self, consumer_id: str) -> int:
        with self._connect() as db:
            row = db.execute(
                "SELECT last_sequence FROM event_consumers WHERE consumer_id=?",
                (consumer_id,),
            ).fetchone()
        return int(row["last_sequence"]) if row else 0

    def count(self) -> int:
        with self._connect() as db:
            return int(db.execute("SELECT COUNT(*) FROM platform_events").fetchone()[0])

    @staticmethod
    def _record(row: sqlite3.Row) -> EventRecord:
        event = PlatformEvent(
            event_type=row["event_type"],
            source=row["source"],
            payload=json.loads(row["payload_json"]),
            schema_version=row["schema_version"],
            classification=row["classification"],
            evidence_refs=json.loads(row["evidence_refs_json"]),
            correlation_id=row["correlation_id"],
            causation_id=row["causation_id"],
            event_id=row["event_id"],
            occurred_at=row["occurred_at"],
        )
        return EventRecord(sequence=int(row["sequence"]), event=event)



def publish_outbox(queue: Any, bus: SQLiteEventBus, *, limit: int = 100) -> list[EventReceipt]:
    """Relay committed Open-Swarm lifecycle events into the local event journal.

    If the process crashes after publish but before marking the outbox row,
    retrying is safe because the event ID is stable and publication is idempotent.
    Run one relay per queue database; the destination journal deduplicates repeats.
    """
    receipts: list[EventReceipt] = []
    for record in queue.pending_outbox(limit):
        queue.record_outbox_attempt(record["event_id"])
        event = PlatformEvent(
            event_id=record["event_id"],
            event_type=record["event_type"],
            source="open-swarm",
            classification="internal",
            correlation_id=record["task_id"],
            occurred_at=record["created_at"],
            payload={
                "task_id": record["task_id"],
                "detail": record["detail"],
            },
        )
        receipt = bus.publish(event)
        queue.mark_outbox_published(record["event_id"])
        receipts.append(receipt)
    return receipts
