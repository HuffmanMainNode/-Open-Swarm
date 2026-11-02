"""Local encrypted OIX request/result round trip through Open-Swarm."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from oix.envelope import create_message
from oix.identity import generate_identity, load_private_key
from oix.inbox import SQLiteInbox
from oix.policy import CapabilityPolicy
from oix.result_channel import SQLiteResultPublisher
from oix.secure_transport import (
    decrypt_envelope,
    encrypt_envelope,
    generate_encryption_key,
    load_encryption_private_key,
)

from openswarm.models import TaskStatus
from openswarm.oix_handoff import OIXTaskHandoff
from openswarm.queue import TaskQueue
from openswarm.runtime import SwarmRuntime, Worker


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="oix-swarm-secure-") as temp:
        root = Path(temp)
        database = root / "shared.sqlite3"
        peer_dir, local_dir = root / "peer", root / "local"
        peer_card = generate_identity(peer_dir, "Requesting Agent", ["public.research"])
        local_card = generate_identity(local_dir, "Swarm Receiver", ["public.research"])
        peer_id = peer_card["card"]["agent_id"]
        local_id = local_card["card"]["agent_id"]
        peer_sign = load_private_key(peer_dir)
        local_sign = load_private_key(local_dir)
        peer_cert = generate_encryption_key(peer_dir, peer_sign)
        local_cert = generate_encryption_key(local_dir, local_sign)
        peer_box = load_encryption_private_key(peer_dir)
        local_box = load_encryption_private_key(local_dir)

        inbox = SQLiteInbox(database, local_id, CapabilityPolicy({"public.research"}))
        inbox.trust_peer(peer_card)
        request = create_message(
            peer_sign, local_id, "task.request",
            {
                "worker": "general",
                "adapter": "mock",
                "prompt": "Summarize the public OIX protocol purpose in one sentence.",
                "capabilities": ["public.research"],
            },
            capability="public.research",
        )
        sealed_request = encrypt_envelope(
            request, local_cert, local_sign.public_key().public_bytes_raw()
        )
        opened_request = decrypt_envelope(
            sealed_request, local_box, local_cert,
            local_sign.public_key().public_bytes_raw(),
            peer_sign.public_key().public_bytes_raw(), local_id,
        )
        receipt = inbox.receive(opened_request)
        inbox.decide_work(receipt.work_id, approve=True)

        queue = TaskQueue(database)
        handoff = OIXTaskHandoff(queue).handoff_one(receipt.sender, receipt.message_id)
        task = SwarmRuntime(
            queue, workers=[Worker("general", {"public.research"})]
        ).run_once()
        if task is None or task.status != TaskStatus.SUCCEEDED:
            raise RuntimeError("mock task did not succeed")

        publisher = SQLiteResultPublisher(database, local_id, local_sign)
        publisher.decide(task.task_id, approve=True)
        signed_result = publisher.sign_approved(task.task_id)
        sealed_result = publisher.encrypt_approved(
            task.task_id, peer_cert, peer_sign.public_key().public_bytes_raw()
        )
        opened_result = decrypt_envelope(
            sealed_result, peer_box, peer_cert,
            peer_sign.public_key().public_bytes_raw(),
            local_sign.public_key().public_bytes_raw(), peer_id,
        )
        retry = publisher.encrypt_approved(
            task.task_id, peer_cert, peer_sign.public_key().public_bytes_raw()
        )
        if opened_result != signed_result or retry != sealed_result:
            raise RuntimeError("result verification or retry persistence failed")

        print(json.dumps({
            "request_id": receipt.message_id,
            "task_id": handoff.task_id,
            "task_status": task.status.value,
            "request_and_result_encryption": "verified",
            "result_message_id": opened_result["body"]["message_id"],
            "recipient_is_original_peer": opened_result["body"]["recipient"] == peer_id,
            "retry_reuses_same_wrapper": True,
            "network_transport": False,
            "remote_delivery": False,
        }, indent=2))


if __name__ == "__main__":
    main()
