# OIX to Open-Swarm atomic task handoff

## Purpose

This adapter moves an already verified and operator-approved OIX `task.request` from the OIX inbox into the Open-Swarm task queue. It is deliberately implemented in Open-Swarm and has no runtime dependency on the OIX Python package. The two projects remain independently installable.

## Required deployment shape

Both components must use the **same local SQLite database file**:

- OIX `SQLiteInbox(database=shared_path, ...)`
- Open-Swarm `TaskQueue(database=shared_path)`
- Open-Swarm `OIXTaskHandoff(queue)`

Do not use two database files, network filesystems, cloud-synced folders, or separate hosts for this transaction. SQLite cannot provide one atomic transaction across two files or machines.

## Trust and approval sequence

1. OIX receives and verifies a signed message against a locally trusted peer card.
2. OIX validates recipient, timestamp/expiry, nonce, message ID, message type, and local capability policy.
3. OIX persists `inbox_messages` and `inbound_work` in one SQLite transaction.
4. An operator approves the OIX work item through `SQLiteInbox.decide_work(..., approve=True)`.
5. Open-Swarm's adapter selects only `task.request` work with status `approved`.
6. The adapter validates a deliberately small task payload schema. Unknown fields, shell instructions as separate fields, oversized prompts, invalid worker identifiers, and capability mismatches fail closed.
7. In one `BEGIN IMMEDIATE` transaction, it inserts the Open-Swarm task, audit event, lifecycle outbox event, durable OIX-to-task mapping, and changes OIX work status from `approved` to `dispatched`.
8. The transaction commits. Only then is the handoff receipt returned.

The OIX approval is the human approval gate for this inbound task. The adapter marks the resulting task as approved, but it does not run the task or invoke a model adapter. Open-Swarm's normal worker registry, capability policy, adapter controls, and run-once workflow remain responsible for execution-time enforcement.

## Stable idempotency

The durable key is `(sender, message_id)`, enforced by the `oix_handoffs` primary key. Task IDs are deterministically derived from that key. A repeated handoff returns the original task ID and cannot create another task or outbox event.

## Crash and rollback guarantee

The OIX status transition, task insert, audit row, lifecycle outbox row, and mapping row all use the same SQLite connection and transaction. If any write fails, SQLite rolls all of them back. The OIX item remains `approved` and can be retried. If commit succeeds but the caller loses the response, retry finds the mapping and returns the original receipt.

This is atomic local handoff, not exactly-once model execution. Task execution remains governed by Open-Swarm's lease/retry model; external side effects performed by an adapter still need their own idempotency protections.

## Payload contract

Accepted payload object fields:

- `prompt`: required string, 1 to 16,000 characters
- `worker`: optional identifier, defaults to `general`
- `adapter`: optional string, defaults to `mock`
- `model`: optional string
- `capabilities`: optional list of 1 to 32 unique labels; defaults to the OIX-approved capability
- `max_attempts`: optional integer from 1 to 10, defaults to 2

The capabilities list must contain exactly the single OIX-approved requested capability. Remote payloads cannot add additional privileges. The adapter does not treat a remote worker or adapter label as trusted merely because it was signed. Open-Swarm must still have the named worker and adapter locally configured.

## Security boundaries

- No network listener, network transport, remote shell, or automatic worker execution is introduced.
- Evidence submissions remain in OIX review and are not converted into tasks.
- This adapter does not read raw OIX envelopes or verify signatures; it accepts only work rows already accepted by the OIX inbox.
- OIX v0.1 messages are signed but not encrypted. Do not send secrets, private prompts, personal data, restricted investigations, or internal search queries.
- A valid signature proves key possession, not truth.
- The shared SQLite file is single-host local storage. Protect it with OS permissions and encrypted storage appropriate to the data classification.


## CLI

After OIX has accepted and an operator has approved task requests in the shared database:

```bash
openswarm --db .openswarm/state.sqlite3 oix-handoff --limit 100
```

The command prints durable handoff receipts. It does not run tasks. Inspect the local queue and audit log separately:

```bash
openswarm --db .openswarm/state.sqlite3 tasks
openswarm --db .openswarm/state.sqlite3 audit --limit 50
```


## End-to-end local demonstration

The repository includes `examples/oix_handoff_demo.py`. It creates two temporary Ed25519 identities, signs a real OIX `task.request`, verifies it through `SQLiteInbox`, explicitly approves it, hands it into Open-Swarm using the shared database, and executes it with the deterministic mock adapter.

Install both projects in one environment, then run:

```bash
python -m pip install -e ../Open-Intelligence-Exchange
python -m pip install -e .
python examples/oix_handoff_demo.py
```

The example creates only temporary keys and a temporary database, uses no hosted model, and opens no network port. It demonstrates request acceptance and local task-result generation. A signed `task.result` return message and live network transport are separate future work; this adapter does not claim remote result delivery.


## Approval-gated result return staging

Phase 3 adds a durable result outbox for tasks created through this OIX handoff.

When an OIX-originated task reaches `succeeded`, `failed`, or `blocked`, Open-Swarm inserts one `oix_result_outbox` row in the same SQLite transaction as terminal task state and the audit event. Local tasks without an `oix_handoffs` mapping do not create OIX result rows. Result rows begin in `pending_approval`; task completion never authorizes release.

The OIX repository provides `SQLiteResultPublisher` to:
1. list pending results for human inspection;
2. approve or reject each result;
3. sign an approved `task.result` envelope and persist the exact envelope and message ID;
4. expose persisted signed envelopes through `pending_delivery()`;
5. record delivery only after a future transport has authenticated the recipient's acknowledgement.

The signed payload includes the task ID, original request message ID, terminal status, bounded result text, and an explicit `result_omitted` flag. It does not automatically include the original prompt or error string. Results over 256 KiB are staged without result text rather than silently truncated.

Example OIX-side use:

```python
from oix.result_channel import SQLiteResultPublisher

publisher = SQLiteResultPublisher(shared_database, local_agent_id, local_private_key)
for record in publisher.pending():
    print(record["task_id"], record["payload"])

publisher.decide(task_id, approve=True)
envelope = publisher.sign_approved(task_id)
# A future transport may transmit this exact persisted envelope.
# Do not mark delivered until authenticated receipt is confirmed.
```

**No transport is implemented.** The signed result remains local in the outbox. A valid signature does not encrypt the payload or prove its truth. OIX v0.1 is signed but not encrypted; never approve release of secrets, personal data, restricted investigations, private prompts, credentials, or private search queries. Operator approval is a release gate, not an automated privacy classifier.

The cross-repository `examples/oix_result_demo.py` verifies the complete local path using real OIX signatures and the Open-Swarm mock adapter. It confirms the return signature and original recipient, but explicitly reports `remote_delivery=false`.


## Encrypted request and result round trip

The Phase 4 interoperability example, `examples/oix_secure_result_demo.py`, extends the local exchange with authenticated encryption:

1. The requesting peer signs an OIX `task.request` and encrypts it to the receiver's Ed25519-bound X25519 certificate.
2. The receiver decrypts the wrapper, verifies the inner signature and recipient, then passes the recovered message to the ordinary durable OIX inbox.
3. The operator approves the request; Open-Swarm receives and executes it with the local mock adapter.
4. The result is staged for operator approval, signed by OIX, and encrypted to the original requesting peer's certified key.
5. The peer decrypts the result and verifies the original recipient and signature. Repeated encryption calls reuse the persisted wrapper.

This proves local confidentiality and integrity across the OIX/Open-Swarm boundary. It still does not open a network listener, transmit data, or claim remote delivery. The static recipient encryption key does not provide forward secrecy against later key compromise.


## Phase 5: authenticated delivery receipt

The encrypted round-trip example now continues beyond decryption:

1. The requesting peer writes the recovered signed `task.result` into its own durable `SQLiteInbox`.
2. Only after that inbox accepts the message does the peer create a signed OIX `delivery.receipt`.
3. The receipt binds the exact logical result message ID to the SHA-256 digest of the encrypted wrapper.
4. The sender-side `SQLiteResultPublisher` verifies the peer's trusted Ed25519 signature, route, message ID, and encrypted-wrapper digest before changing the outbox state to `delivered`.
5. Replaying the exact same verified receipt is idempotent; a different receipt cannot overwrite the delivery record.

The example is still entirely local. It simulates the two peers as independent identities and SQLite inboxes, but does not send data over a network or prove real-world remote availability.


## Phase 6: one-time-prekey exchange

The cross-repository secure round trip now uses signed one-time X25519 prekey bundles in both directions:

- The requesting peer encrypts its signed `task.request` to a prekey published by the local Open-Swarm receiver.
- The receiver authenticates, decrypts, verifies, and atomically consumes that prekey before the ordinary OIX inbox and approval gate accept the request.
- Open-Swarm executes the approved task locally.
- The result publisher persists the approved signed `task.result` encrypted to a prekey from the original peer's signed bundle.
- The peer decrypts and consumes that prekey, stores the signed result in its own inbox, then returns a digest-bound signed receipt.
- The sender verifies the receipt before finalizing delivery.

The example also checks that a retry reuses the exact persisted encrypted wrapper. Prekey private material is local to each peer and never included in the public bundle. This remains a local demonstration; it does not provide bundle discovery, remote reservation, replenishment automation, or network transport. Forward-secrecy benefits depend on secure deletion and the absence of recoverable key copies in backups or storage snapshots.


The Phase 7 OIX integration additionally places each peer's prekey table and inbox tables in the same SQLite database. `consume_and_receive()` performs AEAD decryption, inner-signature verification, inbox policy/trust checks, durable inbox insertion, and one-time-prekey deletion in a single SQLite transaction. If inbox validation or persistence fails, the transaction rolls back and the prekey remains available. This closes the crash window where a key could be consumed before the message was durably accepted.


## Phase 7: mutual-TLS network interoperability

The new example, `examples/oix_mutual_tls_e2e_demo.py`, exercises the OIX Phase 8 framed transport across two independent OIX agent identities and two separate SQLite databases.

The demonstration:
1. Creates independent peer and receiver identities, each with its own inbox and one-time-prekey store.
2. Creates short-lived test TLS certificates signed by a temporary test CA.
3. Starts two OIX mutual-TLS listeners bound only to loopback.
4. Sends an encrypted `task.request` over a real TCP/TLS connection and verifies the receiver's signed, digest-bound receipt.
5. Confirms durable OIX inbox acceptance, applies explicit operator approval, and atomically hands the task into Open-Swarm.
6. Runs the task using the deterministic local mock worker.
7. Stages and approves the result, encrypts it to the original peer's one-time prekey, and sends it over the second mutual-TLS connection.
8. Verifies the peer's signed receipt and confirms that the peer's independent inbox durably stores the `task.result`.

The example reports `physical_hosts=1`: it proves a network exchange between separate agent identities and databases over loopback, not a WAN or multi-host deployment. It uses no hosted model, paid API, central relay, or public listener.

The interoperability workflow checks out OIX Phase 8 and runs this demonstration alongside the prior local, signed, and encrypted handoff demonstrations. The Open-Swarm queue and OIX receiver inbox share one local database for the atomic handoff; the requesting peer remains on its own database.


## Phase 8: certificate-bound agent identity

The mutual-TLS interoperability example now uses separate server certificates for the two OIX agents. Each server and client certificate contains exactly one URI SAN of the form `urn:oix:agent:ed25519:<sha256-fingerprint>`, matching the agent identity represented by that certificate.

OIX Phase 9 checks both directions:
- The receiver rejects a TLS client certificate whose URI SAN does not match the sender identity in the encrypted OIX envelope.
- The sender rejects a TLS server certificate whose URI SAN does not match the expected OIX recipient.

This adds an identity-consistency check between the CA-authenticated TLS peer and the Ed25519-authenticated OIX message. It does not replace OIX signature verification or trusted-peer authorization. Production CA enrollment must validate the signed agent card and proof of possession before issuing the URI SAN; the test CA in this example is temporary and local.


## Phase 9: persistent multi-message streams

The mutual-TLS interoperability example now sends two distinct encrypted task requests over one authenticated peer session, and sends the two resulting encrypted task results over a second session in the reverse direction.

Each stream item uses a distinct signed one-time-prekey bundle. The sender waits for the corresponding signed, digest-bound receipt before transmitting the next item. This exercises bounded persistent sessions, sequential backpressure, durable inbox acceptance, and receipt verification across the Open-Swarm handoff.

The example reports one connection for the request batch and one connection for the result batch. It remains a same-host loopback test with two independent agent databases and does not claim a WAN deployment or a pub/sub broker.


## Phase 10: approval-gated OIX evidence ingestion

Open-Swarm now includes an optional `OIXEvidenceIngestor` adapter in `src/openswarm/oix_evidence.py`. It moves a reviewed OIX `evidence.submit` record into the durable `SQLiteEventBus` as `intelligence.evidence.submitted`.

Requirements and guarantees:
- The OIX inbox and Open-Swarm event journal must use the same local SQLite database file. The adapter refuses separate database paths because cross-database writes would not be atomic.
- Only an OIX `evidence.submit` item with status `approved` may be ingested. Pending and rejected records remain untouched.
- The adapter rechecks the stored envelope signature, sender identity, recipient, message ID, and evidence schema before publishing.
- One `BEGIN IMMEDIATE` transaction appends the platform event, records the ingestion audit, and changes the OIX work status from `approved` to `ingested`.
- Event IDs are deterministic for the sender/message pair. Retrying an already ingested record returns the original event receipt rather than creating a second event.
- The sender's evidence classification is preserved as metadata and explicitly marked sender-asserted. It never downgrades the local event handling level: sender labels `public` and `internal` map to local `internal`, while `restricted` remains `restricted`.
- Only metadata and artifact hashes are exchanged. The adapter does not fetch source URLs, download artifacts, execute code, or interpret evidence as instructions.

Install OIX alongside Open-Swarm to use this optional adapter. The Open-Swarm core remains independently usable without OIX. The cross-repository workflow checks out OIX Phase 11, runs dedicated adapter regression tests, and exercises a persistent mutual-TLS stream containing two task requests and one evidence submission. Task requests remain operator-approved before Open-Swarm execution; evidence requires a separate review approval before it enters the event journal.

This remains a single-host SQLite integration demonstration, not a distributed broker or multi-host production deployment.


## Phase 11: Halloween OSINT and Cyber Observatory evidence sources

The cross-repository demonstration `examples/osint_ecosystem_e2e_demo.py` checks that the optional evidence bridges in both source repositories produce OIX records that can travel through the persistent mutual-TLS stream and enter the Open-Swarm event journal.

The workflow checks out:
- Open Intelligence Exchange Phase 11
- Halloween OSINT `integration/oix-evidence-bridge`
- Huffman Cyber Observatory `integration/oix-evidence-bridge`
- Open-Swarm Phase 11

The demonstration creates synthetic local artifacts, then uses the Halloween bridge to create a `domain_observation` evidence message and the Cyber Observatory bridge to create an `sbom_analysis` evidence message. It sends those alongside two task requests in one authenticated persistent stream. The receiver applies operator approval separately to task requests and evidence records, ingests both evidence events, runs the two approved tasks using the local mock worker, and returns two encrypted results over one persistent stream.

No real target is scanned, no external URL is fetched, and no artifact bytes are uploaded. The demo runs on one physical host with loopback-only listeners and independent OIX agent databases. It demonstrates contract interoperability, not WAN deployment or distributed-broker availability.


## Current trust revalidation at the dispatch boundary

Before moving an approved OIX task into the Open-Swarm queue, the adapter now checks that the sender still has an entry in OIX's local trusted-peer table. Removing a peer from that table after message acceptance and operator approval therefore prevents a not-yet-dispatched task from entering the queue. The bulk handoff query also excludes senders that are no longer trusted.

This is a local, same-SQLite-database check. It does not create a distributed revocation service, cancel tasks that were already dispatched, or replace AAP-style scoped authorization. Already-dispatched tasks follow the existing task lifecycle and result-approval boundary. Operators must separately cancel or block in-flight work when required by their policy.
