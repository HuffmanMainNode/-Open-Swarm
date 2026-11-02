"""Four-repository OSINT evidence and agent exchange demonstration.

Uses synthetic local artifacts from Halloween OSINT and Huffman Cyber
Observatory, signs them through each project's optional OIX bridge, streams
them with two task requests over one mutual-TLS session, ingests approved
evidence into Open-Swarm, executes only approved tasks with a local mock
worker, and returns two encrypted results over one persistent session.
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
import tempfile
from pathlib import Path

from halloween_osint.oix_bridge import create_evidence_message as create_halloween_evidence
from huffman_cyber.oix_bridge import create_evidence_message as create_cyber_evidence
from oix.envelope import create_message
from oix.identity import generate_identity, load_private_key
from oix.inbox import SQLiteInbox
from oix.policy import CapabilityPolicy
from oix.prekeys import SQLitePreKeyStore, create_prekey_bundle, encrypt_prekey_envelope
from oix.result_channel import SQLiteResultPublisher
from oix.transport import OIXMutualTLSServer, send_encrypted_stream

from openswarm.events import SQLiteEventBus
from openswarm.models import TaskStatus
from openswarm.oix_evidence import OIXEvidenceIngestor
from openswarm.oix_handoff import OIXTaskHandoff
from openswarm.queue import TaskQueue
from openswarm.runtime import SwarmRuntime, Worker

from oix_mutual_tls_e2e_demo import _client_tls, _server_tls, _tls_material


async def _run(root: Path) -> dict[str, object]:
    peer_dir, local_dir = root / "osint-agent", root / "open-swarm-agent"
    peer_card = generate_identity(
        peer_dir, "Independent OSINT Evidence Agent", ["public.research"]
    )
    local_card = generate_identity(
        local_dir, "Open-Swarm Evidence Receiver", ["public.research"]
    )
    peer_id = peer_card["card"]["agent_id"]
    local_id = local_card["card"]["agent_id"]
    peer_sign, local_sign = load_private_key(peer_dir), load_private_key(local_dir)

    local_db, peer_db = root / "local-agent.sqlite3", root / "peer-agent.sqlite3"
    local_inbox = SQLiteInbox(
        local_db, local_id, CapabilityPolicy({"public.research"})
    )
    local_inbox.trust_peer(peer_card)
    peer_inbox = SQLiteInbox(peer_db, peer_id)
    peer_inbox.trust_peer(local_card)
    local_prekeys, peer_prekeys = SQLitePreKeyStore(local_db), SQLitePreKeyStore(peer_db)

    domain_artifact = root / "halloween-domain-observation.json"
    domain_artifact.write_text(
        json.dumps(
            {
                "hostname": "example.org",
                "observed_at": "2026-11-02T00:00:00Z",
                "dns": {"addresses": ["203.0.113.10"]},
                "tls": {"verified_by_local_trust_store": True},
                "synthetic": True,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    sbom_artifact = root / "cyber-observatory-sbom-report.json"
    sbom_artifact.write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "sbom_format": "CycloneDX",
                "component_count": 12,
                "finding_count": 1,
                "findings": [
                    {
                        "cve_id": "CVE-2026-12345",
                        "match_type": "explicit_sbom_cve_reference",
                        "requires_manual_triage": True,
                    }
                ],
                "synthetic": True,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    halloween_evidence = create_halloween_evidence(
        identity_dir=peer_dir,
        recipient=local_id,
        artifact_path=domain_artifact,
        evidence_type="domain_observation",
        method_name="synthetic-passive-dns-tls-observation",
        method_version="1.0",
        summary={"hostname": "example.org", "synthetic": True},
        limitations=[
            "Synthetic observation; no real network lookup was performed."
        ],
        classification="public",
        status="unverified",
    )
    cyber_evidence = create_cyber_evidence(
        identity_dir=peer_dir,
        recipient=local_id,
        artifact_path=sbom_artifact,
        evidence_type="sbom_analysis",
        method_name="synthetic-explicit-sbom-kev-correlation",
        method_version="1.0",
        summary={
            "component_count": 12,
            "finding_count": 1,
            "synthetic": True,
        },
        limitations=[
            "Synthetic report; an SBOM match does not prove exploitability."
        ],
        classification="internal",
        status="derived",
    )
    task_requests = [
        create_message(
            peer_sign,
            local_id,
            "task.request",
            {
                "worker": "general",
                "adapter": "mock",
                "prompt": prompt,
                "capabilities": ["public.research"],
            },
            capability="public.research",
        )
        for prompt in (
            "Summarize the purpose of the Open Intelligence Exchange.",
            "Explain why evidence requires independent review.",
        )
    ]
    inbound_messages = [*task_requests, halloween_evidence, cyber_evidence]

    (
        ca,
        local_server_cert,
        local_server_key,
        peer_server_cert,
        peer_server_key,
        peer_cert,
        peer_key,
        local_cert,
        local_key,
    ) = _tls_material(root, peer_id, local_id)
    local_server = OIXMutualTLSServer(local_inbox, local_prekeys, local_sign)
    peer_server = OIXMutualTLSServer(peer_inbox, peer_prekeys, peer_sign)
    local_host, local_port = await local_server.start(
        host="127.0.0.1",
        port=0,
        ssl_context=_server_tls(ca, local_server_cert, local_server_key),
    )
    peer_host, peer_port = await peer_server.start(
        host="127.0.0.1",
        port=0,
        ssl_context=_server_tls(ca, peer_server_cert, peer_server_key),
    )
    peer_client = _client_tls(ca, peer_cert, peer_key)
    local_client = _client_tls(ca, local_cert, local_key)

    try:
        local_bundles = [
            create_prekey_bundle(local_prekeys, local_sign, count=1)
            for _ in inbound_messages
        ]
        sealed_inbound = [
            encrypt_prekey_envelope(
                message,
                local_bundles[index],
                local_sign.public_key().public_bytes_raw(),
            )
            for index, message in enumerate(inbound_messages)
        ]
        request_receipts = await send_encrypted_stream(
            sealed_inbound,
            expected_message_ids=[
                message["body"]["message_id"] for message in inbound_messages
            ],
            host=local_host,
            port=local_port,
            server_hostname="localhost",
            ssl_context=peer_client,
            local_agent_id=peer_id,
            expected_peer_agent_id=local_id,
            expected_peer_signing_public_key=local_sign.public_key().public_bytes_raw(),
        )
        pending = local_inbox.pending_work()
        pending_by_message = {item["message_id"]: item for item in pending}
        if len(pending_by_message) != len(inbound_messages):
            raise RuntimeError("not all task and evidence messages reached the durable inbox")

        for request in task_requests:
            work = pending_by_message[request["body"]["message_id"]]
            local_inbox.decide_work(work["work_id"], approve=True)
        evidence_messages = [halloween_evidence, cyber_evidence]
        for evidence in evidence_messages:
            work = pending_by_message[evidence["body"]["message_id"]]
            if work["work_type"] != "evidence.submit":
                raise RuntimeError("source adapter message was not classified as evidence")
            local_inbox.decide_work(work["work_id"], approve=True)

        event_bus = SQLiteEventBus(local_db)
        ingestor = OIXEvidenceIngestor(local_inbox, event_bus)
        evidence_receipts = [
            ingestor.ingest_one(peer_id, evidence["body"]["message_id"])
            for evidence in evidence_messages
        ]

        queue = TaskQueue(local_db)
        handoff = OIXTaskHandoff(queue)
        handoffs = [
            handoff.handoff_one(peer_id, request["body"]["message_id"])
            for request in task_requests
        ]
        runtime = SwarmRuntime(
            queue, workers=[Worker("general", {"public.research"})]
        )
        tasks = [runtime.run_once() for _ in handoffs]
        if any(task is None or task.status != TaskStatus.SUCCEEDED for task in tasks):
            raise RuntimeError("approved Open-Swarm mock tasks did not all succeed")

        publisher = SQLiteResultPublisher(local_db, local_id, local_sign)
        peer_bundles = [
            create_prekey_bundle(peer_prekeys, peer_sign, count=1)
            for _ in tasks
        ]
        signed_results, sealed_results = [], []
        for index, task in enumerate(tasks):
            publisher.decide(task.task_id, approve=True)
            signed = publisher.sign_approved(task.task_id)
            sealed = publisher.encrypt_approved_with_prekey(
                task.task_id,
                peer_bundles[index],
                peer_sign.public_key().public_bytes_raw(),
            )
            if publisher.encrypt_approved_with_prekey(
                task.task_id,
                peer_bundles[index],
                peer_sign.public_key().public_bytes_raw(),
            ) != sealed:
                raise RuntimeError("result retry did not reuse the persisted envelope")
            signed_results.append(signed)
            sealed_results.append(sealed)

        result_receipts = await send_encrypted_stream(
            sealed_results,
            expected_message_ids=[
                result["body"]["message_id"] for result in signed_results
            ],
            host=peer_host,
            port=peer_port,
            server_hostname="localhost",
            ssl_context=local_client,
            local_agent_id=local_id,
            expected_peer_agent_id=peer_id,
            expected_peer_signing_public_key=peer_sign.public_key().public_bytes_raw(),
        )
        for task, receipt in zip(tasks, result_receipts, strict=True):
            if not publisher.mark_delivered(
                task.task_id, receipt, peer_sign.public_key().public_bytes_raw()
            ):
                raise RuntimeError("result delivery receipt did not verify")

        with sqlite3.connect(local_db) as db:
            event_rows = db.execute(
                "SELECT event_type,classification,payload_json FROM platform_events ORDER BY sequence"
            ).fetchall()
        with sqlite3.connect(peer_db) as db:
            result_count = db.execute(
                "SELECT COUNT(*) FROM inbox_messages WHERE sender=? AND message_type='task.result'",
                (local_id,),
            ).fetchone()[0]

        evidence_types = [
            json.loads(row[2])["evidence"]["evidence_type"]
            for row in event_rows
        ]
        if set(evidence_types) != {"domain_observation", "sbom_analysis"}:
            raise RuntimeError("both source repositories' evidence was not ingested")
        return {
            "source_repositories": [
                "Halloween-OSINT-2026",
                "Huffman-Cyber-Observatory",
            ],
            "inbound_stream_frames": len(inbound_messages),
            "inbound_signed_receipts_verified": len(request_receipts),
            "task_requests_approved": len(task_requests),
            "tasks_succeeded": len(tasks),
            "evidence_records_approved_and_ingested": len(evidence_receipts),
            "evidence_types_ingested": sorted(evidence_types),
            "local_event_classifications": [row[1] for row in event_rows],
            "result_stream_frames": len(sealed_results),
            "result_receipts_verified": len(result_receipts),
            "peer_results_durably_received": result_count,
            "agent_databases_independent": True,
            "physical_hosts": 1,
            "network_transport": "mutual_tls_persistent_loopback",
            "model_adapter": "mock",
            "artifact_bytes_uploaded": False,
        }
    finally:
        await local_server.close()
        await peer_server.close()


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="osint-ecosystem-e2e-") as temp:
        print(json.dumps(asyncio.run(_run(Path(temp))), indent=2))


if __name__ == "__main__":
    main()
