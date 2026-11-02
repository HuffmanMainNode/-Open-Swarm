"""Local OIX one-time-prekey request/result round trip through Open-Swarm."""
from __future__ import annotations

import json
import tempfile
import time
from pathlib import Path

from oix.envelope import create_message
from oix.identity import generate_identity, load_private_key
from oix.inbox import SQLiteInbox
from oix.policy import CapabilityPolicy
from oix.prekeys import (
    SQLitePreKeyStore,
    create_prekey_bundle,
)
from oix.receipts import create_delivery_receipt, message_digest
from oix.result_channel import SQLiteResultPublisher

from openswarm.models import TaskStatus
from openswarm.oix_handoff import OIXTaskHandoff
from openswarm.queue import TaskQueue
from openswarm.runtime import SwarmRuntime, Worker


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="oix-swarm-prekeys-") as temp:
        root = Path(temp)
        database = root / "shared.sqlite3"
        peer_dir, local_dir = root / "peer", root / "local"
        peer_card = generate_identity(peer_dir, "Requesting Agent", ["public.research"])
        local_card = generate_identity(local_dir, "Swarm Receiver", ["public.research"])
        peer_id = peer_card["card"]["agent_id"]
        local_id = local_card["card"]["agent_id"]
        peer_sign = load_private_key(peer_dir)
        local_sign = load_private_key(local_dir)

        # Each side publishes signed public prekeys while retaining private
        # one-time keys in its own local, owner-only SQLite store.
        peer_prekeys = SQLitePreKeyStore(root / "peer-prekeys.sqlite3")
        local_prekeys = SQLitePreKeyStore(root / "local-prekeys.sqlite3")
        peer_bundle = create_prekey_bundle(peer_prekeys, peer_sign, count=8)
        local_bundle = create_prekey_bundle(local_prekeys, local_sign, count=8)

        inbox = SQLiteInbox(database, local_id, CapabilityPolicy({"public.research"}))
        inbox.trust_peer(peer_card)
        request = create_message(
            peer_sign,
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
        sealed_request = encrypt_prekey_envelope(
            request, local_bundle, local_sign.public_key().public_bytes_raw()
        )
        opened_request = local_prekeys.consume_and_decrypt(
            sealed_request,
            local_sign.public_key().public_bytes_raw(),
            peer_sign.public_key().public_bytes_raw(),
            local_id,
        )
        received = inbox.receive(opened_request)
        inbox.decide_work(received.work_id, approve=True)

        queue = TaskQueue(database)
        handoff = OIXTaskHandoff(queue).handoff_one(
            received.sender, received.message_id
        )
        task = SwarmRuntime(
            queue, workers=[Worker("general", {"public.research"})]
        ).run_once()
        if task is None or task.status != TaskStatus.SUCCEEDED:
            raise RuntimeError("mock task did not succeed")

        publisher = SQLiteResultPublisher(database, local_id, local_sign)
        publisher.decide(task.task_id, approve=True)
        signed_result = publisher.sign_approved(task.task_id)
        sealed_result = publisher.encrypt_approved_with_prekey(
            task.task_id,
            peer_bundle,
            peer_sign.public_key().public_bytes_raw(),
        )
        retry = publisher.encrypt_approved_with_prekey(
            task.task_id,
            peer_bundle,
            peer_sign.public_key().public_bytes_raw(),
        )
        if retry != sealed_result:
            raise RuntimeError("retry did not reuse the persisted one-time-prekey wrapper")

        opened_result = peer_prekeys.consume_and_decrypt(
            sealed_result,
            peer_sign.public_key().public_bytes_raw(),
            local_sign.public_key().public_bytes_raw(),
            peer_id,
        )
        if opened_result != signed_result:
            raise RuntimeError("one-time-prekey result decryption or signature verification failed")

        # The peer durably stores the result before signing a digest-bound receipt.
        peer_inbox = SQLiteInbox(root / "peer-inbox.sqlite3", peer_id)
        peer_inbox.trust_peer(local_card)
        accepted = peer_inbox.receive(opened_result)
        if accepted.message_id != opened_result["body"]["message_id"]:
            raise RuntimeError("peer inbox did not accept the expected result")
        delivery_receipt = create_delivery_receipt(
            peer_sign,
            local_id,
            opened_result["body"]["message_id"],
            message_digest(sealed_result),
            accepted_at=int(time.time()),
        )
        delivered = publisher.mark_delivered(
            task.task_id,
            delivery_receipt,
            peer_sign.public_key().public_bytes_raw(),
        )
        duplicate_receipt = publisher.mark_delivered(
            task.task_id,
            delivery_receipt,
            peer_sign.public_key().public_bytes_raw(),
        )
        if not delivered or duplicate_receipt:
            raise RuntimeError("signed receipt lifecycle was not idempotent")

        print(json.dumps({
            "request_message_id": received.message_id,
            "task_id": handoff.task_id,
            "task_status": task.status.value,
            "request_one_time_prekey": "consumed_after_verified_decryption",
            "result_one_time_prekey": "consumed_after_verified_decryption",
            "result_message_id": opened_result["body"]["message_id"],
            "recipient_is_original_peer": opened_result["body"]["recipient"] == peer_id,
            "retry_reused_same_encrypted_wrapper": True,
            "peer_inbox_durable_acceptance": True,
            "signed_receipt_verified": True,
            "delivery_state": "delivered",
            "transport": "local_only",
            "remote_delivery": False,
            "model_adapter": "mock",
        }, indent=2))


if __name__ == "__main__":
    main()
