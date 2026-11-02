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
from oix.transport import OIXMutualTLSServer, send_encrypted_envelope

from openswarm.models import TaskStatus
from openswarm.oix_handoff import OIXTaskHandoff
from openswarm.queue import TaskQueue
from openswarm.runtime import SwarmRuntime, Worker

def _tls_material(root: Path) -> tuple[Path, ...]:
    """Create a short-lived test CA, server cert, and two client certificates."""
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

    def issue(name: str, usage, *, server: bool = False):
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
        builder = (
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
        )
        if server:
            builder = builder.add_extension(
                x509.SubjectAlternativeName([
                    x509.DNSName("localhost"),
                    x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                ]),
                critical=False,
            )
        cert = builder.sign(ca_key, hashes.SHA256())
        cert_path = root / f"{name}.pem"
        key_path = root / f"{name}.key"
        cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        key_path.write_bytes(key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        ))
        return cert_path, key_path

    server_cert, server_key = issue(
        "oix-loopback-server", ExtendedKeyUsageOID.SERVER_AUTH, server=True
    )
    peer_cert, peer_key = issue("oix-peer-client", ExtendedKeyUsageOID.CLIENT_AUTH)
    local_cert, local_key = issue("oix-local-client", ExtendedKeyUsageOID.CLIENT_AUTH)
    return ca_path, server_cert, server_key, peer_cert, peer_key, local_cert, local_key


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
    local_bundle = create_prekey_bundle(local_prekeys, local_sign, count=4)
    peer_bundle = create_prekey_bundle(peer_prekeys, peer_sign, count=4)

    ca, server_cert, server_key, peer_cert, peer_key, local_cert, local_key = _tls_material(root)
    local_server = OIXMutualTLSServer(local_inbox, local_prekeys, local_sign)
    peer_server = OIXMutualTLSServer(peer_inbox, peer_prekeys, peer_sign)
    local_host, local_port = await local_server.start(
        host="127.0.0.1", port=0, ssl_context=_server_tls(ca, server_cert, server_key)
    )
    peer_host, peer_port = await peer_server.start(
        host="127.0.0.1", port=0, ssl_context=_server_tls(ca, server_cert, server_key)
    )
    peer_client = _client_tls(ca, peer_cert, peer_key)
    local_client = _client_tls(ca, local_cert, local_key)

    try:
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
        request_receipt = await send_encrypted_envelope(
            sealed_request,
            host=local_host,
            port=local_port,
            server_hostname="localhost",
            ssl_context=peer_client,
            local_agent_id=peer_id,
            expected_peer_agent_id=local_id,
            expected_message_id=request["body"]["message_id"],
            expected_peer_signing_public_key=local_sign.public_key().public_bytes_raw(),
        )
        if request_receipt["body"]["type"] != "delivery.receipt":
            raise RuntimeError("receiver did not return a signed request receipt")

        pending = local_inbox.pending_work()
        if len(pending) != 1 or pending[0]["message_id"] != request["body"]["message_id"]:
            raise RuntimeError("request was not durably accepted into the OIX inbox")
        local_inbox.decide_work(pending[0]["work_id"], approve=True)

        queue = TaskQueue(local_db)
        handoff = OIXTaskHandoff(queue).handoff_one(peer_id, request["body"]["message_id"])
        task = SwarmRuntime(
            queue, workers=[Worker("general", {"public.research"})]
        ).run_once()
        if task is None or task.status != TaskStatus.SUCCEEDED:
            raise RuntimeError("Open-Swarm mock task did not succeed")

        publisher = SQLiteResultPublisher(local_db, local_id, local_sign)
        publisher.decide(task.task_id, approve=True)
        signed_result = publisher.sign_approved(task.task_id)
        sealed_result = publisher.encrypt_approved_with_prekey(
            task.task_id, peer_bundle, peer_sign.public_key().public_bytes_raw()
        )
        if publisher.encrypt_approved_with_prekey(
            task.task_id, peer_bundle, peer_sign.public_key().public_bytes_raw()
        ) != sealed_result:
            raise RuntimeError("result retry did not reuse its persisted encrypted envelope")

        result_receipt = await send_encrypted_envelope(
            sealed_result,
            host=peer_host,
            port=peer_port,
            server_hostname="localhost",
            ssl_context=local_client,
            local_agent_id=local_id,
            expected_peer_agent_id=peer_id,
            expected_message_id=signed_result["body"]["message_id"],
            expected_peer_signing_public_key=peer_sign.public_key().public_bytes_raw(),
        )
        if not publisher.mark_delivered(
            task.task_id, result_receipt, peer_sign.public_key().public_bytes_raw()
        ):
            raise RuntimeError("first verified delivery receipt did not finalize result")

        with sqlite3.connect(peer_db) as db:
            row = db.execute(
                """SELECT message_type,content_sha256 FROM inbox_messages
                   WHERE sender=? AND message_id=?""",
                (local_id, signed_result["body"]["message_id"]),
            ).fetchone()
        if row is None or row[0] != "task.result":
            raise RuntimeError("peer inbox did not durably store the task.result message")

        return {
            "request_message_id": request["body"]["message_id"],
            "request_receipt_verified": True,
            "task_id": handoff.task_id,
            "task_status": task.status.value,
            "result_message_id": signed_result["body"]["message_id"],
            "result_receipt_verified": True,
            "peer_result_inbox_durable": True,
            "result_retry_reused_envelope": True,
            "network_transport": "mutual_tls_loopback",
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
