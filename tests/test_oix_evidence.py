from __future__ import annotations

import json
import sqlite3

import pytest

pytest.importorskip("oix")

from oix.envelope import create_message
from oix.identity import generate_identity, load_private_key
from oix.inbox import SQLiteInbox
from oix.policy import CapabilityPolicy

from openswarm.events import SQLiteEventBus
from openswarm.oix_evidence import OIXEvidenceIngestError, OIXEvidenceIngestor


def evidence_payload() -> dict:
    return {
        "schema_version": "1.0",
        "evidence_id": "4b3b7a6f-3e5a-4a6d-9f0b-2d9f1c6a7e80",
        "evidence_type": "domain_observation",
        "source_uri": "https://example.org/security-advisory",
        "source_timestamp": None,
        "collected_at": "2026-11-02T00:00:00Z",
        "collector": {"name": "halloween-osint-2026", "version": "0.1.0"},
        "method": {"name": "dns-tls-observation", "version": "1.0"},
        "artifact": {
            "sha256": "a" * 64,
            "size_bytes": 2048,
            "media_type": "application/json",
        },
        "classification": "public",
        "status": "observed",
        "summary": {"record_count": 2, "observation_kind": "domain_infrastructure"},
        "limitations": [
            "The digest identifies the artifact bytes but does not prove source authenticity."
        ],
    }


def setup_exchange(tmp_path):
    sender_dir = tmp_path / "sender"
    receiver_dir = tmp_path / "receiver"
    sender_card = generate_identity(sender_dir, "Evidence Sender", ["public.research"])
    receiver_card = generate_identity(receiver_dir, "Open-Swarm", ["public.research"])
    sender_key = load_private_key(sender_dir)
    sender_id = sender_card["card"]["agent_id"]
    receiver_id = receiver_card["card"]["agent_id"]
    database = tmp_path / "shared.sqlite3"
    inbox = SQLiteInbox(database, receiver_id, CapabilityPolicy({"public.research"}))
    inbox.trust_peer(sender_card)
    bus = SQLiteEventBus(database)
    message = create_message(
        sender_key,
        receiver_id,
        "evidence.submit",
        evidence_payload(),
        capability="public.research",
    )
    receipt = inbox.receive(message)
    ingestor = OIXEvidenceIngestor(inbox, bus)
    return database, inbox, bus, ingestor, message, receipt, sender_id


def test_evidence_ingestion_requires_operator_approval(tmp_path):
    _, _, _, ingestor, message, receipt, sender_id = setup_exchange(tmp_path)
    assert receipt.work_status == "pending_review"
    with pytest.raises(OIXEvidenceIngestError, match="explicit operator approval"):
        ingestor.ingest_one(sender_id, message["body"]["message_id"])


def test_approved_evidence_atomically_becomes_classified_platform_event(tmp_path):
    database, inbox, bus, ingestor, message, receipt, sender_id = setup_exchange(tmp_path)
    work = inbox.pending_work()[0]
    assert inbox.decide_work(work["work_id"], approve=True) == "approved"

    result = ingestor.ingest_one(sender_id, message["body"]["message_id"])
    assert result.duplicate is False
    assert result.evidence_id == evidence_payload()["evidence_id"]

    with sqlite3.connect(database) as db:
        row = db.execute(
            """SELECT event_type,source,classification,evidence_refs_json,payload_json
               FROM platform_events WHERE event_id=?""",
            (result.event_id,),
        ).fetchone()
        status = db.execute(
            "SELECT status FROM inbound_work WHERE work_id=?", (work["work_id"],)
        ).fetchone()[0]
    assert row is not None
    assert row[0] == "intelligence.evidence.submitted"
    assert row[1] == f"oix:{sender_id}"
    assert row[2] == "public"
    assert json.loads(row[3]) == [f"oix:evidence:{evidence_payload()['evidence_id']}"]
    event_payload = json.loads(row[4])
    assert event_payload["evidence"] == evidence_payload()
    assert event_payload["classification_is_sender_asserted"] is True
    assert "artifact_bytes" not in event_payload
    assert status == "ingested"

    retry = ingestor.ingest_one(sender_id, message["body"]["message_id"])
    assert retry.duplicate is True
    assert retry.event_id == result.event_id
    assert retry.sequence == result.sequence


def test_rejected_evidence_cannot_be_ingested(tmp_path):
    _, inbox, _, ingestor, message, _, sender_id = setup_exchange(tmp_path)
    work = inbox.pending_work()[0]
    assert inbox.decide_work(work["work_id"], approve=False) == "rejected"
    with pytest.raises(OIXEvidenceIngestError, match="explicit operator approval"):
        ingestor.ingest_one(sender_id, message["body"]["message_id"])


def test_modified_stored_envelope_fails_signature_validation(tmp_path):
    database, inbox, bus, ingestor, message, _, sender_id = setup_exchange(tmp_path)
    work = inbox.pending_work()[0]
    inbox.decide_work(work["work_id"], approve=True)
    with sqlite3.connect(database) as db:
        envelope = json.loads(db.execute(
            "SELECT envelope_json FROM inbox_messages WHERE sender=? AND message_id=?",
            (sender_id, message["body"]["message_id"]),
        ).fetchone()[0])
        envelope["body"]["payload"]["summary"]["record_count"] = 999
        db.execute(
            "UPDATE inbox_messages SET envelope_json=? WHERE sender=? AND message_id=?",
            (json.dumps(envelope), sender_id, message["body"]["message_id"]),
        )
    with pytest.raises(OIXEvidenceIngestError, match="integrity or schema validation"):
        ingestor.ingest_one(sender_id, message["body"]["message_id"])
    with sqlite3.connect(database) as db:
        assert db.execute("SELECT COUNT(*) FROM platform_events").fetchone()[0] == 0
        assert db.execute(
            "SELECT status FROM inbound_work WHERE work_id=?", (work["work_id"],)
        ).fetchone()[0] == "approved"


def test_event_and_inbox_must_share_database(tmp_path):
    sender_dir = tmp_path / "sender"
    receiver_dir = tmp_path / "receiver"
    sender_card = generate_identity(sender_dir, "Sender", ["public.research"])
    receiver_card = generate_identity(receiver_dir, "Receiver", ["public.research"])
    inbox = SQLiteInbox(tmp_path / "inbox.sqlite3", receiver_card["card"]["agent_id"])
    inbox.trust_peer(sender_card)
    bus = SQLiteEventBus(tmp_path / "events.sqlite3")
    with pytest.raises(ValueError, match="share one SQLite database"):
        OIXEvidenceIngestor(inbox, bus)


def test_event_publish_failure_rolls_back_ingestion_state(tmp_path, monkeypatch):
    database, inbox, bus, ingestor, message, _, sender_id = setup_exchange(tmp_path)
    work = inbox.pending_work()[0]
    inbox.decide_work(work["work_id"], approve=True)

    def fail(*_args, **_kwargs):
        raise RuntimeError("simulated event journal failure")

    monkeypatch.setattr(bus, "publish_in_transaction", fail)
    with pytest.raises(RuntimeError, match="simulated event journal failure"):
        ingestor.ingest_one(sender_id, message["body"]["message_id"])
    with sqlite3.connect(database) as db:
        assert db.execute("SELECT COUNT(*) FROM platform_events").fetchone()[0] == 0
        assert db.execute(
            "SELECT COUNT(*) FROM oix_evidence_ingest_audit"
        ).fetchone()[0] == 0
        assert db.execute(
            "SELECT status FROM inbound_work WHERE work_id=?", (work["work_id"],)
        ).fetchone()[0] == "approved"
