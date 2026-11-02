"""Supervised local outbox relay with a SQLite-backed single-instance lease.

This service coordinates one relay process per task database. It does not
provide distributed delivery or encryption. The destination event journal
continues to provide idempotent publication and at-least-once semantics.
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import asdict, dataclass
from uuid import uuid4

from openswarm.events import PlatformEvent, SQLiteEventBus
from openswarm.queue import TaskQueue

logger = logging.getLogger("openswarm.relay")


class RelayAlreadyRunning(RuntimeError):
    """Another live process currently owns the relay lease."""


@dataclass(frozen=True, slots=True)
class RelayResult:
    published: int
    duplicates: int
    pending: int


class OutboxRelay:
    def __init__(
        self,
        queue: TaskQueue,
        bus: SQLiteEventBus,
        *,
        batch_size: int = 100,
        lease_seconds: int = 60,
    ) -> None:
        if not 1 <= batch_size <= 1000:
            raise ValueError("batch_size must be between 1 and 1000")
        if not 10 <= lease_seconds <= 3600:
            raise ValueError("lease_seconds must be between 10 and 3600")
        self.queue = queue
        self.bus = bus
        self.batch_size = batch_size
        self.lease_seconds = lease_seconds
        self.owner = str(uuid4())
        with self.queue._connect() as db:
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS relay_lease (
                    lease_name TEXT PRIMARY KEY,
                    owner TEXT NOT NULL,
                    lease_until REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )

    def _acquire(self) -> None:
        now = time.time()
        with self.queue._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT owner,lease_until FROM relay_lease WHERE lease_name='outbox'"
            ).fetchone()
            if row is not None and row["lease_until"] > now and row["owner"] != self.owner:
                raise RelayAlreadyRunning("another relay process holds the active lease")
            db.execute(
                """
                INSERT INTO relay_lease(lease_name,owner,lease_until,updated_at)
                VALUES('outbox',?,?,?)
                ON CONFLICT(lease_name) DO UPDATE SET
                    owner=excluded.owner,
                    lease_until=excluded.lease_until,
                    updated_at=excluded.updated_at
                """,
                (self.owner, now + self.lease_seconds, now),
            )

    def _renew(self) -> None:
        now = time.time()
        with self.queue._connect() as db:
            changed = db.execute(
                """
                UPDATE relay_lease SET lease_until=?,updated_at=?
                WHERE lease_name='outbox' AND owner=?
                """,
                (now + self.lease_seconds, now, self.owner),
            ).rowcount
        if not changed:
            raise RelayAlreadyRunning("relay lease was lost; stopping publication")

    def _release(self) -> None:
        with self.queue._connect() as db:
            db.execute(
                "DELETE FROM relay_lease WHERE lease_name='outbox' AND owner=?",
                (self.owner,),
            )

    def status(self) -> dict[str, int | float | None | bool]:
        now = time.time()
        with self.queue._connect() as db:
            row = db.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE published_at IS NULL) AS pending,
                    COUNT(*) FILTER (WHERE published_at IS NOT NULL) AS published,
                    COALESCE(SUM(attempts),0) AS attempts,
                    MIN(created_at) FILTER (WHERE published_at IS NULL) AS oldest_pending
                FROM integration_outbox
                """
            ).fetchone()
            lease = db.execute(
                "SELECT lease_until FROM relay_lease WHERE lease_name='outbox'"
            ).fetchone()
        oldest_age = None
        if row["oldest_pending"]:
            from datetime import datetime

            created = datetime.fromisoformat(row["oldest_pending"].replace("Z", "+00:00"))
            oldest_age = max(0.0, now - created.timestamp())
        return {
            "pending": int(row["pending"]),
            "published": int(row["published"]),
            "attempts": int(row["attempts"]),
            "oldest_pending_age_seconds": oldest_age,
            "relay_lease_active": bool(lease and lease["lease_until"] > now),
        }

    def run_once(self) -> RelayResult:
        self._acquire()
        published = 0
        duplicates = 0
        try:
            records = self.queue.pending_outbox(self.batch_size)
            for record in records:
                self._renew()
                self.queue.record_outbox_attempt(record["event_id"])
                event = PlatformEvent(
                    event_id=record["event_id"],
                    event_type=record["event_type"],
                    source="open-swarm",
                    classification="internal",
                    correlation_id=record["task_id"],
                    occurred_at=record["created_at"],
                    payload={"task_id": record["task_id"], "detail": record["detail"]},
                )
                receipt = self.bus.publish(event)
                self.queue.mark_outbox_published(record["event_id"])
                if receipt.duplicate:
                    duplicates += 1
                else:
                    published += 1
            pending = len(self.queue.pending_outbox(self.batch_size))
            return RelayResult(published, duplicates, pending)
        finally:
            self._release()

    def run_forever(
        self,
        *,
        poll_interval: float = 2.0,
        stop_event: threading.Event | None = None,
    ) -> None:
        if not 0.1 <= poll_interval <= 300:
            raise ValueError("poll_interval must be between 0.1 and 300 seconds")
        stop = stop_event or threading.Event()
        logger.info("outbox relay started")
        while not stop.is_set():
            try:
                result = self.run_once()
                if result.published or result.duplicates:
                    logger.info("relay batch completed: %s", asdict(result))
            except RelayAlreadyRunning:
                logger.exception("relay lease conflict; this process will stop")
                raise
            except Exception as exc:  # keep service alive after transient local I/O failures
                logger.error("relay iteration failed (%s); retrying", type(exc).__name__)
            stop.wait(poll_interval)
        logger.info("outbox relay stopped")
