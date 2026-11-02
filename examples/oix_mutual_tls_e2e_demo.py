"""Cross-agent OIX mutual-TLS request/result exchange through Open-Swarm.

Both OIX agents use separate SQLite databases and distinct Ed25519 identities.
The demo opens only loopback listeners, uses short-lived test certificates,
and runs the Open-Swarm mock worker. It exercises real TCP/TLS framing, OIX
encryption, durable inbox acceptance, operator approval, task execution,
encrypted result return, and authenticated delivery receipts.
"""
from __future__ import annotations

import asyncio
import ipaddress
import json
import sqlite3
import ssl
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from oix.envelope import create_message
from oix.identity import generate_identity, load_private_key
from oix.inbox import SQLiteInbox
from oix.policy import CapabilityPolicy
from oix.prekeys import SQLitePreKeyStore, create_prekey_bundle, encrypt_prekey_envelope
from oix.result_channel import SQLiteResultPublisher
from oix.transport import (
    OIXMutualTLSServer,
    agent_identity_uri,
    send_encrypted_stream,
)

from openswarm.models import TaskStatus
from openswarm.oix_handoff import OIXTaskHandoff
from openswarm.queue import TaskQueue
from openswarm.runtime import SwarmRuntime, Worker


def _tls_material(
    root: Path, peer_agent_id: str, local_agent_id: str
) -> tuple[Path, ...]:
    """Create a short-lived test CA and identity-bound server/client certificates."""
    now = datetime.now(UTC)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "OIX Loopback Test CA")])
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=False,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(ca_key, hashes.SHA256())
    )
    ca_path = root / "test-ca.pem"
    ca_path.write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    ca_ski = ca_cert.extensions.get_extension_for_class(x509.SubjectKeyIdentifier).value

    def issue(name: str, usage, agent_id: str, *, server: bool = False):
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
        san_names = [
            x509.UniformResourceIdentifier(agent_identity_uri(agent_id))
        ]
        if server:
            san_names.extend([
                x509.DNSName("localhost"),
                x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
            ])
        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(ca_name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=1))
            .not_valid_after(now + timedelta(hours=12))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(
                x509.AuthorityKeyIdentifier.from_issuer_subject_key_identifier(ca_ski),
                critical=False,
            )
            .add_extension(x509.ExtendedKeyUsage([usage]), critical=False)
            .add_extension(x509.SubjectAlternativeName(san_names), critical=False)
            .sign(ca_key, hashes.SHA256())
        )
        cert_path = root / f"{name}.pem"
        key_path = root / f"{name}.key"
        cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        key_path.write_bytes(key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        ))
        return cert_path, key_path

    local_server_cert, local_server_key = issue(
        "oix-local-server", ExtendedKeyUsageOID.SERVER_AUTH, local_agent_id, server=True
    )
    peer_server_cert, peer_server_key = issue(
        "oix-peer-server", ExtendedKeyUsageOID.SERVER_AUTH, peer_agent_id, server=True
    )
    peer_client_cert, peer_client_key = issue(
        "oix-peer-client", ExtendedKeyUsageOID.CLIENT_AUTH, peer_agent_id
    )
    local_client_cert, local_client_key = issue(
        "oix-local-client", ExtendedKeyUsageOID.CLIENT_AUTH, local_agent_id
    )
    return (
        ca_path,
        local_server_cert,
        local_server_key,
        peer_server_cert,
        peer_server_key,
        peer_client_cert,
        peer_client_key,
        local_client_cert,
        local_client_key,
    )


def _server_tls(ca: Path, cert: Path, key: Path) -> ssl.SSLContext:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    context.load_verify_locations(cafile=str(ca))
    context.verify_mode = ssl.CERT_REQUIRED
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    return context


def _client_tls(ca: Path, cert: Path, key: Path) -> ssl.SSLContext:
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=str(ca))
    context.load_cert_chain(cert, key)
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    return context


async def _exchange(root: Path) -> dict[str, object]:
    peer_dir, local_dir = root / "peer-identity", root / "local-identity"
    peer_card = generate_identity(peer_dir, "Independent Requesting Agent", ["public.research"])
    local_card = generate_identity(local_dir, "Open-Swarm Receiver", ["public.research"])
    peer_id, local_id = peer_card["card"]["agent_id"], local_card["card"]["agent_id"]
    peer_sign, local_sign = load_private_key(peer_dir), load_private_key(local_dir)

    # The peers have independent databases. The local OIX inbox and Open-Swarm
    # task queue share one database only for the documented atomic handoff.
    local_db, peer_db = root / "local-agent.sqlite3", root / "peer-agent.sqlite3"
    local_inbox = SQLiteInbox(local_db, local_id, CapabilityPolicy({"public.research"}))
    local_inbox.trust_peer(peer_card)
    peer_inbox = SQLiteInbox(peer_db, peer_id)
    peer_inbox.trust_peer(local_card)
    local_prekeys, peer_prekeys = SQLitePreKeyStore(local_db), SQLitePreKeyStore(peer_db)

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
        host="127.0.0.1", port=0,
        ssl_context=_server_tls(ca, local_server_cert, local_server_key)
    )
    peer_host, peer_port = await peer_server.start(
        host="127.0.0.1", port=0,
        ssl_context=_server_tls(ca, peer_server_cert, peer_server_key)
    )
    peer_client = _client_tls(ca, peer_cert, peer_key)
    local_client = _client_tls(ca, local_cert, local_key)

    try:
        requests = [
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
                "Summarize the public OIX protocol purpose in one sentence.",
                "List the OIX trust checks that precede task approval.",
            )
        ]
        local_bundles = [
            create_prekey_bundle(local_prekeys, local_sign, count=1)
            for _ in requests
        ]
        peer_bundles = [
            create_prekey_bundle(peer_prekeys, peer_sign, count=1)
            for _ in requests
        ]
        sealed_requests = [
            encrypt_prekey_envelope(
                request,
                local_bundles[index],
                local_sign.public_key().public_bytes_raw(),
            )
            for index, request in enumerate(requests)
        ]
        request_receipts = await send_encrypted_stream(
            sealed_requests,
            expected_message_ids=[
                request["body"]["message_id"] for request in requests
            ],
            host=local_host,
            port=local_port,
            server_hostname="localhost",
            ssl_context=peer_client,
            local_agent_id=peer_id,
            expected_peer_agent_id=local_id,
            expected_peer_signing_public_key=local_sign.public_key().public_bytes_raw(),
        )
        if len(request_receipts) != len(requests) or any(
            receipt["body"]["type"] != "delivery.receipt"
            for receipt in request_receipts
        ):
            raise RuntimeError("request stream did not return every signed receipt")

        pending = local_inbox.pending_work()
        pending_by_message = {item["message_id"]: item for item in pending}
        if len(pending_by_message) != len(requests):
            raise RuntimeError("request stream was not durably accepted into the OIX inbox")
        for request in requests:
            work = pending_by_message.get(request["body"]["message_id"])
            if work is None:
                raise RuntimeError("a streamed request is missing from the durable inbox")
            local_inbox.decide_work(work["work_id"], approve=True)

        queue = TaskQueue(local_db)
        handoff_adapter = OIXTaskHandoff(queue)
        handoffs = [
            handoff_adapter.handoff_one(peer_id, request["body"]["message_id"])
            for request in requests
        ]
        runtime = SwarmRuntime(
            queue, workers=[Worker("general", {"public.research"})]
        )
        tasks = [runtime.run_once() for _ in requests]
        if any(task is None or task.status != TaskStatus.SUCCEEDED for task in tasks):
            raise RuntimeError("one or more Open-Swarm mock tasks did not succeed")

        publisher = SQLiteResultPublisher(local_db, local_id, local_sign)
        signed_results = []
        sealed_results = []
        for index, task in enumerate(tasks):
            publisher.decide(task.task_id, approve=True)
            signed = publisher.sign_approved(task.task_id)
            sealed = publisher.encrypt_approved_with_prekey(
                task.task_id,
                peer_bundles[index],
                peer_sign.public_key().public_bytes_raw(),
            )
            retry = publisher.encrypt_approved_with_prekey(
                task.task_id,
                peer_bundles[index],
                peer_sign.public_key().public_bytes_raw(),
            )
            if retry != sealed:
                raise RuntimeError("result retry did not reuse its persisted encrypted envelope")
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
        if len(result_receipts) != len(signed_results):
            raise RuntimeError("result stream did not return every signed receipt")
        for task, receipt in zip(tasks, result_receipts, strict=True):
            if not publisher.mark_delivered(
                task.task_id, receipt, peer_sign.public_key().public_bytes_raw()
            ):
                raise RuntimeError("verified delivery receipt did not finalize result")

        result_ids = [result["body"]["message_id"] for result in signed_results]
        with sqlite3.connect(peer_db) as db:
            placeholders = ",".join("?" for _ in result_ids)
            rows = db.execute(
                f"""SELECT message_id,message_type FROM inbox_messages
                    WHERE sender=? AND message_id IN ({placeholders})""",
                [local_id, *result_ids],
            ).fetchall()
        if len(rows) != len(result_ids) or any(row[1] != "task.result" for row in rows):
            raise RuntimeError("peer inbox did not durably store every streamed task.result")

        return {
            "request_count": len(requests),
            "request_stream_receipts_verified": len(request_receipts),
            "task_ids": [handoff.task_id for handoff in handoffs],
            "task_statuses": [task.status.value for task in tasks],
            "result_count": len(signed_results),
            "result_stream_receipts_verified": len(result_receipts),
            "peer_results_durably_accepted": len(rows),
            "result_retries_reused_persisted_envelopes": True,
            "network_transport": "mutual_tls_persistent_loopback_streams",
            "request_stream_connections": 1,
            "result_stream_connections": 1,
            "agent_databases_independent": True,
            "physical_hosts": 1,
            "model_adapter": "mock",
            "task_execution": "local_operator_approved",
        }
    finally:
        await local_server.close()
        await peer_server.close()


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="oix-swarm-mutual-tls-") as temp:
        result = asyncio.run(_exchange(Path(temp)))
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
