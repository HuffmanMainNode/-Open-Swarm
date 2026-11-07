from __future__ import annotations

import hashlib
import json
import os
import secrets
import sqlite3
import time
import uuid
from pathlib import Path

import pytest

from hdi.assurance import build_receipt, canonical_json, evaluate_receipt, verify_receipt
from oix.envelope import create_message
from oix.identity import generate_identity, load_private_key
from oix.inbox import SQLiteInbox
from oix.policy import CapabilityPolicy
from oix.result_channel import ResultChannelError, SQLiteResultPublisher
from openswarm.models import TaskStatus
from openswarm.oix_handoff import OIXHandoffError, OIXTaskHandoff
from openswarm.queue import TaskQueue
from openswarm.runtime import SwarmRuntime, Worker

class RevokingAdapter:
    def __init__(self, inbox: SQLiteInbox, peer_id: str):
        self.inbox = inbox
        self.peer_id = peer_id
        self.calls = 0

    def generate(self, prompt: str, model: str | None = None, timeout_seconds: int = 60) -> str:
        self.calls += 1
        self.inbox.revoke_peer(self.peer_id)
        return "This output must not survive a concurrent peer revocation."


def _stack(root: Path, suffix: str):
    root.mkdir(parents=True, exist_ok=True)
    database = root / "shared.sqlite3"
    peer_dir = root / "peer"
    local_dir = root / "local"
    peer_card = generate_identity(peer_dir, f"peer-{suffix}", ["public.research"])
    local_card = generate_identity(local_dir, f"local-{suffix}", ["public.research"])
    peer_key = load_private_key(peer_dir)
    local_key = load_private_key(local_dir)
    peer_id = peer_card["card"]["agent_id"]
    local_id = local_card["card"]["agent_id"]
    inbox = SQLiteInbox(
        database, local_id, CapabilityPolicy({"public.research"})
    )
    inbox.trust_peer(peer_card)
    queue = TaskQueue(database)
    return {
        "database": database,
        "peer_card": peer_card,
        "peer_key": peer_key,
        "peer_id": peer_id,
        "local_id": local_id,
        "local_key": local_key,
        "inbox": inbox,
        "queue": queue,
    }


def _receive_and_approve(stack, prompt: str):
    message = create_message(
        stack["peer_key"],
        stack["local_id"],
        "task.request",
        {
            "prompt": prompt,
            "worker": "general",
            "adapter": "mock",
            "capabilities": ["public.research"],
        },
        capability="public.research",
    )
    received = stack["inbox"].receive(message)
    stack["inbox"].decide_work(received.work_id, approve=True)
    return received


def _evidence_digest(value: dict) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def test_oix_openswarm_revocation_flow_emits_verifiable_huffman_receipt(tmp_path):
    observations = {}

    # Scenario 1: revocation invalidates an approval before task dispatch.
    before = _stack(tmp_path / "before", "before")
    received = _receive_and_approve(before, "This approved task must be revoked.")
    before["inbox"].revoke_peer(before["peer_id"])
    with pytest.raises(OIXHandoffError, match="no longer trusted"):
        OIXTaskHandoff(before["queue"]).handoff_one(
            received.sender, received.message_id
        )
    with sqlite3.connect(before["database"]) as db:
        before_state = {
            "work_status": db.execute(
                "SELECT status FROM inbound_work WHERE work_id=?",
                (received.work_id,),
            ).fetchone()[0],
            "task_count": db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0],
        }
    assert before_state == {"work_status": "revoked", "task_count": 0}
    before["inbox"].trust_peer(before["peer_card"])
    with pytest.raises(OIXHandoffError):
        OIXTaskHandoff(before["queue"]).handoff_one(
            received.sender, received.message_id
        )
    observations["before_dispatch"] = before_state

    # Scenario 2: revocation during inference discards the generated result.
    during = _stack(tmp_path / "during", "during")
    received = _receive_and_approve(during, "Revoke this peer while the adapter runs.")
    handoff = OIXTaskHandoff(during["queue"]).handoff_one(
        received.sender, received.message_id
    )
    adapter = RevokingAdapter(during["inbox"], during["peer_id"])
    runtime = SwarmRuntime(
        during["queue"], workers=[Worker("general", {"public.research"})]
    )
    runtime.register_adapter("mock", adapter)
    completed = runtime.run_once()
    assert completed is not None
    assert completed.task_id == handoff.task_id
    assert completed.status == TaskStatus.BLOCKED
    assert "trust revoked" in (completed.error or "")
    assert adapter.calls == 1
    with sqlite3.connect(during["database"]) as db:
        during_state = {
            "task_status": db.execute(
                "SELECT status FROM tasks WHERE task_id=?", (handoff.task_id,)
            ).fetchone()[0],
            "result_outbox_count": db.execute(
                "SELECT COUNT(*) FROM oix_result_outbox"
            ).fetchone()[0],
        }
    assert during_state == {
        "task_status": TaskStatus.BLOCKED.value,
        "result_outbox_count": 0,
    }
    observations["during_inference"] = during_state

    # Scenario 3: revocation rejects an already-staged, undelivered result.
    after = _stack(tmp_path / "after", "after")
    received = _receive_and_approve(after, "Complete this task before revocation.")
    handoff = OIXTaskHandoff(after["queue"]).handoff_one(
        received.sender, received.message_id
    )
    runtime = SwarmRuntime(
        after["queue"], workers=[Worker("general", {"public.research"})]
    )
    completed = runtime.run_once()
    assert completed is not None and completed.status == TaskStatus.SUCCEEDED
    publisher = SQLiteResultPublisher(
        after["database"], after["local_id"], after["local_key"]
    )
    with sqlite3.connect(after["database"]) as db:
        assert db.execute(
            "SELECT status FROM oix_result_outbox WHERE task_id=?",
            (handoff.task_id,),
        ).fetchone()[0] == "pending_approval"
    after["inbox"].revoke_peer(after["peer_id"])
    with sqlite3.connect(after["database"]) as db:
        after_state = {
            "result_status": db.execute(
                "SELECT status FROM oix_result_outbox WHERE task_id=?",
                (handoff.task_id,),
            ).fetchone()[0],
            "rejection_reason": db.execute(
                "SELECT rejection_reason FROM oix_result_outbox WHERE task_id=?",
                (handoff.task_id,),
            ).fetchone()[0],
        }
    assert after_state == {
        "result_status": "rejected",
        "rejection_reason": "recipient peer revoked",
    }
    assert publisher.pending_delivery() == []
    with pytest.raises(ResultChannelError):
        publisher.sign_approved(handoff.task_id)
    observations["after_execution"] = after_state

    # Produce a portable receipt bound to the exact suite source and dependency commits.
    test_source = Path(__file__).read_bytes()
    manifest = {
        "suite_id": "huffman.oix-openswarm.revocation",
        "suite_version": "0.1.0",
        "test_source_sha256": hashlib.sha256(test_source).hexdigest(),
        "components": {
            "oix": os.environ.get("HUFFMAN_OIX_SHA", "local-unpinned"),
            "open_swarm": os.environ.get("GITHUB_SHA", "local-working-tree"),
            "hdi": os.environ.get("HUFFMAN_HDI_SHA", "local-unpinned"),
        },
    }
    suite_digest = hashlib.sha256(canonical_json(manifest)).hexdigest()
    verifier_key = Ed25519PrivateKey.generate()
    nonce = secrets.token_hex(32)
    now = int(time.time())
    checks = []
    for check_id, requirement, evidence in [
        (
            "REV-001",
            "Revocation before dispatch invalidates prior approval, including after re-trust.",
            observations["before_dispatch"],
        ),
        (
            "REV-002",
            "Revocation during inference blocks task completion and suppresses output.",
            observations["during_inference"],
        ),
        (
            "REV-003",
            "Revocation rejects pending result delivery and prevents later signing.",
            observations["after_execution"],
        ),
    ]:
        checks.append({
            "check_id": check_id,
            "requirement": requirement,
            "outcome": "PASS",
            "evidence": [{"kind": "sqlite-state", "sha256": _evidence_digest(evidence)}],
        })

    receipt = build_receipt(
        verifier_key,
        subject={
            "subject_id": "integration:oix-openswarm",
            "implementation": "OIX + Open-Swarm + HDI",
            "version": "integration-0.1.0",
        },
        suite={
            "suite_id": manifest["suite_id"],
            "version": manifest["suite_version"],
            "sha256": suite_digest,
        },
        run_id=str(uuid.uuid4()),
        challenge_nonce=nonce,
        checks=checks,
        issued_at=now,
        ttl_seconds=3600,
    )
    public_key = verifier_key.public_key().public_bytes_raw()
    assert verify_receipt(receipt, public_key, now=now, expected_nonce=nonce)
    assert evaluate_receipt(
        receipt,
        public_key,
        {"REV-001", "REV-002", "REV-003"},
        now=now,
        expected_nonce=nonce,
    ) == "PASS"

    receipt_dir = Path(os.environ.get("HUFFMAN_RECEIPT_DIR", str(tmp_path)))
    receipt_dir.mkdir(parents=True, exist_ok=True)
    (receipt_dir / f"{receipt['receipt_id']}.json").write_text(
        json.dumps(receipt, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
