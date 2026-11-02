"""End-to-end local OIX request -> Open-Swarm -> signed result demonstration.

Install both repositories in one Python environment, then run:
    python examples/oix_result_demo.py

This uses temporary identities and one shared SQLite database. It signs and
verifies the return envelope locally. It does not transmit over a network.
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from oix.envelope import create_message, verify_message
from oix.identity import generate_identity, load_private_key
from oix.inbox import SQLiteInbox
from oix.policy import CapabilityPolicy
from oix.result_channel import SQLiteResultPublisher

from openswarm.models import TaskStatus
from openswarm.oix_handoff import OIXTaskHandoff
from openswarm.queue import TaskQueue
from openswarm.runtime import SwarmRuntime, Worker


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="oix-result-demo-") as temporary:
        root = Path(temporary)
        database = root / "shared.sqlite3"
        peer_dir = root / "peer"
        local_dir = root / "local"

        peer_card = generate_identity(
            peer_dir, "Independent Requesting Agent", ["public.research"]
        )
        local_card = generate_identity(
            local_dir, "Local Open-Swarm Receiver", ["public.research"]
        )
        peer_id = peer_card["card"]["agent_id"]
        local_id = local_card["card"]["agent_id"]
        peer_key = load_private_key(peer_dir)
        local_key = load_private_key(local_dir)

        inbox = SQLiteInbox(
            database, local_id, CapabilityPolicy({"public.research"})
        )
        inbox.trust_peer(peer_card)
        request = create_message(
            peer_key,
            local_id,
            "task.request",
            {
                "worker": "general",
                "adapter": "mock",
                "prompt": "Summarize the public OIX protocol purpose in one sentence.",
                "capabilities": ["public.research"],
            },
            capability="public.research",
        )
        received = inbox.receive(request)
        if received.work_id is None:
            raise RuntimeError("OIX did not create an approval-gated task item")
        inbox.decide_work(received.work_id, approve=True)

        queue = TaskQueue(database)
        handoff = OIXTaskHandoff(queue).handoff_one(
            received.sender, received.message_id
        )
        result = SwarmRuntime(
            queue, workers=[Worker("general", {"public.research"})]
        ).run_once()
        if result is None or result.status != TaskStatus.SUCCEEDED:
            raise RuntimeError("Open-Swarm did not complete the local mock task")

        publisher = SQLiteResultPublisher(database, local_id, local_key)
        pending = publisher.pending()
        if len(pending) != 1 or pending[0]["task_id"] != result.task_id:
            raise RuntimeError("terminal task result was not placed in approval outbox")
        publisher.decide(result.task_id, approve=True)
        signed_result = publisher.sign_approved(result.task_id)

        if not verify_message(
            signed_result, local_key.public_key().public_bytes_raw()
        ):
            raise RuntimeError("signed task.result did not verify")
        if signed_result["body"]["recipient"] != peer_id:
            raise RuntimeError("signed task.result recipient does not match origin peer")

        # The exact signed envelope is persisted. Repeated signing returns it.
        if publisher.sign_approved(result.task_id) != signed_result:
            raise RuntimeError("retry did not reuse the persisted signed envelope")

        print(json.dumps({
            "request_message_id": received.message_id,
            "task_id": handoff.task_id,
            "task_status": result.status.value,
            "result_approval": "approved",
            "result_message_id": signed_result["body"]["message_id"],
            "result_signature": "verified",
            "recipient_matches_origin": True,
            "retry_reused_signed_message": True,
            "transport": "not_configured",
            "remote_delivery": False,
            "model_adapter": "mock",
        }, indent=2))


if __name__ == "__main__":
    main()
