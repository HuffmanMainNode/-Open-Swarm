"""Checkpointed polling supervisor for continuous local-first intelligence ingestion.

Adapters are explicit, code-owned objects. Each poll batch and its checkpoint
advance commit atomically with event publication in the same SQLite database.
This is a bounded polling supervisor, not a distributed stream broker.
"""
from __future__ import annotations

import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol
from uuid import uuid4

from openswarm.events import EventReceipt, PlatformEvent, SQLiteEventBus

MAX_BATCH_EVENTS = 1000
MAX_CHECKPOINT_CHARS = 4096
SOURCE_ID = re.compile(r"^[a-z][a-z0-9_.-]{1,63}$")


class IngestionError(RuntimeError):
    """An ingestion source or supervisor operation failed."""


class IngestionLeaseError(IngestionError):
    """A source lease expired or was replaced before commit."""


@dataclass(frozen=True, slots=True)
class SourceBatch:
    events: list[PlatformEvent]
    checkpoint: str | None


@dataclass(frozen=True, slots=True)
class IngestionRunResult:
    source_id: str
    status: str
    event_count: int
    duplicate_count: int
    error_type: str | None = None


@dataclass(frozen=True, slots=True)
class SourceStatus:
    source_id: str
    checkpoint: str | None
    interval_seconds: int
    next_poll_at: str
    last_success_at: str | None
    last_error_type: str | None
    consecutive_failures: int
    lease_active: bool


class SourceAdapter(Protocol):
    def poll(self, checkpoint: str | None) -> SourceBatch:
        """Return a bounded batch and the checkpoint after all returned events."""


def _timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class IngestionSupervisor:
    """Run explicit polling adapters with leases, checkpoints, backoff, and audit."""

    def __init__(
        self,
        event_bus: SQLiteEventBus,
        *,
        lease_seconds: int = 120,
        max_backoff_seconds: int = 3600,
    ) -> None:
        if not 5 <= lease_seconds <= 3600:
            raise ValueError("lease_seconds must be between 5 and 3600")
        if not 1 <= max_backoff_seconds <= 86_400:
            raise ValueError("max_backoff_seconds must be between 1 and 86400")
        self.event_bus = event_bus
        self.database = str(Path(event_bus.database).resolve())
        self.lease_seconds = lease_seconds
        self.max_backoff_seconds = max_backoff_seconds
        self._adapters: dict[str, SourceAdapter] = {}
        with self.event_bus._connect() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS ingestion_sources (
                    source_id TEXT PRIMARY KEY,
                    checkpoint TEXT,
                    interval_seconds INTEGER NOT NULL,
                    next_poll_at TEXT NOT NULL,
                    last_success_at TEXT,
                    last_error_type TEXT,
                    consecutive_failures INTEGER NOT NULL DEFAULT 0,
                    lease_id TEXT,
                    lease_expires_at TEXT,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ingestion_runs (
                    run_id TEXT PRIMARY KEY,
                    source_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    event_count INTEGER NOT NULL DEFAULT 0,
                    duplicate_count INTEGER NOT NULL DEFAULT 0,
                    error_type TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_ingestion_sources_due
                    ON ingestion_sources(next_poll_at);
                CREATE INDEX IF NOT EXISTS idx_ingestion_runs_source
                    ON ingestion_runs(source_id,started_at);
                """
            )

    def register_source(
        self,
        source_id: str,
        adapter: SourceAdapter,
        *,
        interval_seconds: int = 60,
        now: datetime | None = None,
    ) -> None:
        if not isinstance(source_id, str) or not SOURCE_ID.fullmatch(source_id):
            raise ValueError("source_id must be a lowercase identifier of 2 to 64 characters")
        if not 1 <= interval_seconds <= 86_400:
            raise ValueError("interval_seconds must be between 1 and 86400")
        if not callable(getattr(adapter, "poll", None)):
            raise TypeError("adapter must provide a callable poll(checkpoint) method")
        current = now or datetime.now(UTC)
        current_text = _timestamp(current)
        self._adapters[source_id] = adapter
        with self.event_bus._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                """
                INSERT INTO ingestion_sources(
                    source_id,checkpoint,interval_seconds,next_poll_at,updated_at
                ) VALUES(?,NULL,?,?,?)
                ON CONFLICT(source_id) DO UPDATE SET
                    interval_seconds=excluded.interval_seconds,
                    updated_at=excluded.updated_at
                """,
                (source_id, interval_seconds, current_text, current_text),
            )
            db.commit()

    def run_source_once(
        self,
        source_id: str,
        *,
        force: bool = False,
        now: datetime | None = None,
    ) -> IngestionRunResult:
        adapter = self._adapters.get(source_id)
        if adapter is None:
            raise IngestionError("source adapter is not registered in this process")
        current = now or datetime.now(UTC)
        current_text = _timestamp(current)
        lease_id = str(uuid4())
        run_id = str(uuid4())

        with self.event_bus._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            state = db.execute(
                "SELECT * FROM ingestion_sources WHERE source_id=?", (source_id,)
            ).fetchone()
            if state is None:
                db.rollback()
                raise IngestionError("source must be registered before polling")
            if not force and _parse_timestamp(state["next_poll_at"]) > current:
                db.rollback()
                return IngestionRunResult(source_id, "not_due", 0, 0)
            if (
                state["lease_id"]
                and state["lease_expires_at"]
                and _parse_timestamp(state["lease_expires_at"]) > current
            ):
                db.rollback()
                return IngestionRunResult(source_id, "lease_active", 0, 0)
            checkpoint = state["checkpoint"]
            interval_seconds = int(state["interval_seconds"])
            expires_at = _timestamp(current + timedelta(seconds=self.lease_seconds))
            db.execute(
                """
                UPDATE ingestion_sources
                SET lease_id=?,lease_expires_at=?,updated_at=?
                WHERE source_id=?
                """,
                (lease_id, expires_at, current_text, source_id),
            )
            db.execute(
                """
                INSERT INTO ingestion_runs(run_id,source_id,status,started_at)
                VALUES(?,?,?,?)
                """,
                (run_id, source_id, "running", current_text),
            )
            db.commit()

        try:
            batch = adapter.poll(checkpoint)
            if not isinstance(batch, SourceBatch):
                raise IngestionError("adapter must return a SourceBatch")
            if (
                not isinstance(batch.events, list)
                or len(batch.events) > MAX_BATCH_EVENTS
            ):
                raise IngestionError(
                    f"adapter batch must contain at most {MAX_BATCH_EVENTS} events"
                )
            if batch.checkpoint is not None and (
                not isinstance(batch.checkpoint, str)
                or len(batch.checkpoint) > MAX_CHECKPOINT_CHARS
            ):
                raise IngestionError("checkpoint must be null or a bounded string")
            event_ids = [event.event_id for event in batch.events]
            if len(event_ids) != len(set(event_ids)):
                raise IngestionError("adapter batch contains duplicate event IDs")

            with self.event_bus._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                state = db.execute(
                    "SELECT lease_id FROM ingestion_sources WHERE source_id=?",
                    (source_id,),
                ).fetchone()
                if state is None or state["lease_id"] != lease_id:
                    db.rollback()
                    raise IngestionLeaseError("source lease was lost before commit")
                receipts: list[EventReceipt] = [
                    self.event_bus.publish_in_transaction(db, event)
                    for event in batch.events
                ]
                duplicate_count = sum(1 for receipt in receipts if receipt.duplicate)
                completed_at = _timestamp(datetime.now(UTC))
                next_poll_at = _timestamp(
                    datetime.now(UTC) + timedelta(seconds=interval_seconds)
                )
                db.execute(
                    """
                    UPDATE ingestion_sources
                    SET checkpoint=?,next_poll_at=?,last_success_at=?,
                        last_error_type=NULL,consecutive_failures=0,
                        lease_id=NULL,lease_expires_at=NULL,updated_at=?
                    WHERE source_id=? AND lease_id=?
                    """,
                    (
                        batch.checkpoint, next_poll_at, completed_at,
                        completed_at, source_id, lease_id,
                    ),
                )
                db.execute(
                    """
                    UPDATE ingestion_runs
                    SET status='succeeded',finished_at=?,event_count=?,duplicate_count=?
                    WHERE run_id=?
                    """,
                    (completed_at, len(receipts), duplicate_count, run_id),
                )
                db.commit()
            return IngestionRunResult(
                source_id, "succeeded", len(receipts), duplicate_count
            )
        except Exception as exc:
            finished_at = _timestamp(datetime.now(UTC))
            with self.event_bus._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                state = db.execute(
                    """
                    SELECT consecutive_failures,interval_seconds,lease_id
                    FROM ingestion_sources WHERE source_id=?
                    """,
                    (source_id,),
                ).fetchone()
                if state is not None and state["lease_id"] == lease_id:
                    failures = int(state["consecutive_failures"]) + 1
                    delay = min(
                        int(state["interval_seconds"]) * (2 ** min(failures - 1, 16)),
                        self.max_backoff_seconds,
                    )
                    retry_at = _timestamp(
                        datetime.now(UTC) + timedelta(seconds=delay)
                    )
                    db.execute(
                        """
                        UPDATE ingestion_sources
                        SET next_poll_at=?,last_error_type=?,consecutive_failures=?,
                            lease_id=NULL,lease_expires_at=NULL,updated_at=?
                        WHERE source_id=? AND lease_id=?
                        """,
                        (
                            retry_at, type(exc).__name__, failures, finished_at,
                            source_id, lease_id,
                        ),
                    )
                db.execute(
                    """
                    UPDATE ingestion_runs
                    SET status='failed',finished_at=?,error_type=?
                    WHERE run_id=?
                    """,
                    (finished_at, type(exc).__name__, run_id),
                )
                db.commit()
            return IngestionRunResult(
                source_id, "failed", 0, 0, type(exc).__name__
            )

    def run_due_once(self, *, now: datetime | None = None) -> list[IngestionRunResult]:
        with self.event_bus._connect() as db:
            rows = db.execute(
                "SELECT source_id FROM ingestion_sources ORDER BY source_id"
            ).fetchall()
        return [
            self.run_source_once(row["source_id"], now=now)
            for row in rows
            if row["source_id"] in self._adapters
        ]

    def status(self) -> list[SourceStatus]:
        current = datetime.now(UTC)
        with self.event_bus._connect() as db:
            rows = db.execute(
                "SELECT * FROM ingestion_sources ORDER BY source_id"
            ).fetchall()
        return [
            SourceStatus(
                source_id=row["source_id"],
                checkpoint=row["checkpoint"],
                interval_seconds=int(row["interval_seconds"]),
                next_poll_at=row["next_poll_at"],
                last_success_at=row["last_success_at"],
                last_error_type=row["last_error_type"],
                consecutive_failures=int(row["consecutive_failures"]),
                lease_active=bool(
                    row["lease_id"]
                    and row["lease_expires_at"]
                    and _parse_timestamp(row["lease_expires_at"]) > current
                ),
            )
            for row in rows
        ]

    def run_forever(self, *, idle_sleep_seconds: float = 1.0) -> None:
        if not 0.1 <= idle_sleep_seconds <= 60:
            raise ValueError("idle_sleep_seconds must be between 0.1 and 60")
        while True:
            results = self.run_due_once()
            if not results or all(
                result.status in {"not_due", "lease_active"} for result in results
            ):
                time.sleep(idle_sleep_seconds)
