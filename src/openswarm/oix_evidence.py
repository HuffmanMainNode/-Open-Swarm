"""Approval-gated OIX evidence ingestion into the durable Open-Swarm event journal.

This adapter requires the OIX inbox and event journal to share one local SQLite
file so advancing the approved inbox record and appending its event are atomic.
It records metadata only; it never downloads evidence artifacts or executes
submitted content.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from openswarm.events import EventReceipt, PlatformEvent, SQLiteEventBus


class OIXEvidenceIngestError(RuntimeError):
    """An OIX evidence item cannot be safely ingested."""


@dataclass(frozen=True, slots=True)
class OIXEvidenceReceipt:
    sender: str
    message_id: str
    evidence_id: str
    event_id: str
    sequence: int
    duplicate: bool


class OIXEvidenceIngestor:
    """Move operator-approved OIX evidence.submit records into the event journal."""

    def __init__(self, inbox, event_bus: SQLiteEventBus) -> None:
        self.inbox = inbox
        self.event_bus = event_bus
        inbox_path = Path(inbox.database).resolve()
        event_path = Path(event_bus.database).resolve()
        if inbox_path != event_path:
            raise ValueError(
                "OIX inbox and event journal must share one SQLite database"
            )
        self.database = str(inbox_path)
        with sqlite3.connect(self.database) as db:
            db.execute("PRAGMA foreign_keys=ON")
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS oix_evidence_ingest_audit (
                    work_id TEXT PRIMARY KEY,
                    sender TEXT NOT NULL,
                    message_id TEXT NOT NULL,
                    evidence_id TEXT NOT NULL,
                    event_id TEXT NOT NULL UNIQUE,
                    ingested_at TEXT NOT NULL,
                    FOREIGN KEY(work_id) REFERENCES inbound_work(work_id)
                )
                """
            )
            db.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_oix_evidence_ingest_message
                ON oix_evidence_ingest_audit(sender,message_id)
                """
            )

    def ingest_one(self, sender: str, message_id: str) -> OIXEvidenceReceipt:
        """Atomically publish one approved evidence record and mark it ingested."""
        try:
            from oix.envelope import verify_for_recipient
            from oix.inbox import validate_evidence_submit_payload
        except ImportError as exc:
            raise OIXEvidenceIngestError(
                "install Open Intelligence Exchange to use the OIX evidence adapter"
            ) from exc

        with sqlite3.connect(self.database, timeout=10) as db:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA busy_timeout=10000")
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                """
                SELECT w.work_id,w.sender,w.message_id,w.work_type,w.payload_json,w.status,
                       m.message_type,m.envelope_json
                FROM inbound_work AS w
                JOIN inbox_messages AS m
                  ON m.sender=w.sender AND m.message_id=w.message_id
                WHERE w.sender=? AND w.message_id=?
                """,
                (sender, message_id),
            ).fetchone()
            if row is None:
                db.rollback()
                raise OIXEvidenceIngestError("OIX evidence work item does not exist")

            prior = db.execute(
                """
                SELECT evidence_id,event_id FROM oix_evidence_ingest_audit
                WHERE work_id=?
                """,
                (row["work_id"],),
            ).fetchone()
            if row["status"] == "ingested" and prior is not None:
                event_row = db.execute(
                    "SELECT sequence FROM platform_events WHERE event_id=?",
                    (prior["event_id"],),
                ).fetchone()
                if event_row is None:
                    db.rollback()
                    raise OIXEvidenceIngestError(
                        "ingest audit exists without its durable platform event"
                    )
                db.commit()
                return OIXEvidenceReceipt(
                    sender, message_id, prior["evidence_id"], prior["event_id"],
                    int(event_row["sequence"]), True,
                )
            if row["work_type"] != "evidence.submit" or row["message_type"] != "evidence.submit":
                db.rollback()
                raise OIXEvidenceIngestError("inbound work is not an evidence.submit record")
            if row["status"] != "approved":
                db.rollback()
                raise OIXEvidenceIngestError(
                    "evidence must receive explicit operator approval before ingestion"
                )

            try:
                payload = json.loads(row["payload_json"])
                envelope = json.loads(row["envelope_json"])
                body = envelope["body"]
                if (
                    body.get("sender") != sender
                    or body.get("message_id") != message_id
                    or body.get("type") != "evidence.submit"
                    or body.get("payload") != payload
                ):
                    raise ValueError("stored message and approved work record disagree")
                issued_at = body.get("issued_at")
                public_key = self.inbox.trusted_peer_public_key(sender)
                if not verify_for_recipient(
                    envelope, public_key, self.inbox.local_agent_id, now=issued_at
                ):
                    raise ValueError("stored OIX signature or recipient validation failed")
                validate_evidence_submit_payload(payload)
            except Exception as exc:
                db.rollback()
                raise OIXEvidenceIngestError(
                    "stored OIX evidence failed integrity or schema validation"
                ) from exc

            evidence_id = payload["evidence_id"]
            event_id = str(
                uuid5(NAMESPACE_URL, f"oix:evidence.submit:{sender}:{message_id}")
            )
            event = PlatformEvent(
                event_id=event_id,
                event_type="intelligence.evidence.submitted",
                source=f"oix:{sender}",
                schema_version="1.0",
                # Remote labels never downgrade the local event's handling level.
                classification=(
                    "restricted" if payload["classification"] == "restricted" else "internal"
                ),
                occurred_at=payload["collected_at"],
                evidence_refs=[f"oix:evidence:{evidence_id}"],
                causation_id=message_id,
                payload={
                    "sender": sender,
                    "message_id": message_id,
                    "work_id": row["work_id"],
                    "evidence": payload,
                    "source_asserted_classification": payload["classification"],
                    "classification_is_sender_asserted": True,
                },
            )
            try:
                event_receipt: EventReceipt = self.event_bus.publish_in_transaction(
                    db, event
                )
                db.execute(
                    """
                    INSERT INTO oix_evidence_ingest_audit(
                        work_id,sender,message_id,evidence_id,event_id,ingested_at
                    ) VALUES(?,?,?,?,?,?)
                    """,
                    (
                        row["work_id"], sender, message_id, evidence_id, event_id,
                        event.occurred_at,
                    ),
                )
                changed = db.execute(
                    """
                    UPDATE inbound_work SET status='ingested'
                    WHERE work_id=? AND status='approved'
                    """,
                    (row["work_id"],),
                ).rowcount
                if changed != 1:
                    raise OIXEvidenceIngestError(
                        "approved evidence state changed during ingestion"
                    )
                db.commit()
            except Exception:
                db.rollback()
                raise

            return OIXEvidenceReceipt(
                sender, message_id, evidence_id, event_receipt.event_id,
                event_receipt.sequence, event_receipt.duplicate,
            )

    def ingest_approved(self, *, limit: int = 100) -> list[OIXEvidenceReceipt]:
        """Ingest a bounded batch of approved evidence submissions in creation order."""
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        with sqlite3.connect(self.database) as db:
            rows = db.execute(
                """
                SELECT sender,message_id FROM inbound_work
                WHERE work_type='evidence.submit' AND status='approved'
                ORDER BY created_at,work_id LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self.ingest_one(row[0], row[1]) for row in rows]
