"""End-to-end local OIX request -> operator approval -> Open-Swarm mock result.

Install both repositories in the same Python environment before running:
    python -m pip install -e ../Open-Intelligence-Exchange
    python -m pip install -e .
    python examples/oix_handoff_demo.py

This example uses temporary identities and a temporary shared SQLite database.
It does not open a network port or call a hosted model.
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from oix.envelope import create_message
from oix.identity import generate_identity, load_private_key
from oix.inbox import SQLiteInbox
from oix.policy import CapabilityPolicy

from openswarm.models import TaskStatus
from openswarm.oix_handoff import OIXTaskHandoff
from openswarm.queue import TaskQueue
from openswarm.runtime import SwarmRuntime, Worker


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="oix-swarm-demo-") as temporary:
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
        local_agent_id = local_card["card"]["agent_id"]

        inbox = SQLiteInbox(
            database,
            local_agent_id,
            CapabilityPolicy({"public.research"}),
        )
        inbox.trust_peer(peer_card)

        request = create_message(
            load_private_key(peer_dir),
            local_agent_id,
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

        # Demonstration only: the local operator explicitly approves this request.
        inbox.decide_work(received.work_id, approve=True)

        queue = TaskQueue(database)
        handoff = OIXTaskHandoff(queue).handoff_one(
            received.sender, received.message_id
        )
        result = SwarmRuntime(
            queue,
            workers=[Worker("general", {"public.research"})],
        ).run_once()
        if result is None or result.status != TaskStatus.SUCCEEDED:
            raise RuntimeError("Open-Swarm did not complete the local mock task")

        print(json.dumps({
            "oix_message_id": received.message_id,
            "oix_work_id": received.work_id,
            "handoff_task_id": handoff.task_id,
            "handoff_duplicate": handoff.duplicate,
            "task_status": result.status.value,
            "task_result": result.result,
            "transport": "local-only",
            "model_adapter": "mock",
        }, indent=2))


if __name__ == "__main__":
    main()
