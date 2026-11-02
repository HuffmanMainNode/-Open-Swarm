# OIX-to-platform event mapping

**Status:** Design contract for the next integration phase. This document does not claim that the adapter is implemented.

OIX and the Open-Swarm platform event journal solve different problems:

- **OIX** authenticates and frames messages exchanged between independent agents.
- **The platform event journal** records normalized internal observations and lifecycle events for local consumers.

An OIX message must not be copied blindly into the internal event log. The receiving adapter is a trust boundary.

## Required receive sequence

1. Enforce transport size, timeout, rate, and TLS policy before parsing.
2. Parse the bounded OIX envelope and reject unknown protocol versions or message types.
3. Resolve the sender against a locally trusted agent-card registry. Discovery is not trust.
4. Verify the signed agent card, fingerprint, message signature, timestamp, TTL, and exact recipient.
5. Apply durable replay protection using the (sender, nonce) pair.
6. Apply local capability policy independently of signature validity.
7. Validate the message-type-specific payload against a strict schema.
8. Normalize accepted information into a versioned platform event.
9. Preserve the OIX message ID, sender fingerprint, envelope hash, evidence references, and validation outcome as provenance.
10. Publish the event idempotently. Acknowledge the transport delivery only after the durable local write succeeds.

The adapter must fail closed. Unknown senders, unsupported types, invalid signatures, replayed messages, expired messages, and unauthorized capabilities are rejected and audited without executing payload instructions.

## Mapping table

| OIX message type | Internal event type | Handling |
|---|---|---|
| agent.hello | oix.peer.hello_received | Record peer metadata only after identity verification |
| agent.hello_ack | oix.peer.handshake_completed | Record negotiated protocol and capabilities |
| task.request | swarm.task.request_received | Convert a strict task request to a bounded queued task; approval policy still applies |
| task.accepted | swarm.task.accepted_remote | Record remote acknowledgement |
| task.progress | swarm.task.progress_remote | Record bounded progress metadata |
| task.result | swarm.task.result_received | Store result as untrusted agent output with provenance |
| evidence.submit | intelligence.evidence.received | Normalize evidence metadata; preserve source and collection limitations |
| error | oix.message.error_received | Record a bounded error code and safe diagnostic context |

The internal event type names are proposed names under the platform event contract. They are not OIX protocol message types.

## Task request mapping

A future adapter should accept only an explicit, versioned payload object. The request must identify a registered local worker, contain a bounded prompt or structured task description, and request a capability that the local policy independently permits.

The sender's advertised capability list is not an authorization grant. A signed task.request is not operator approval. Any task requiring approval must remain in Open-Swarm's existing pending-approval state until a local operator approves it.

The adapter must not allow a remote sender to select arbitrary adapters, model endpoints, filesystem paths, shell commands, or privileged tools. Those are local deployment decisions.

## Evidence mapping

An evidence.submit payload must retain:

- Original source URI and source timestamp, if known.
- Collection timestamp, collector identity, collector version, and method.
- Content hash or immutable evidence reference.
- Evidence status and explicit limitations.
- Sender identity and the signed OIX message reference.

A valid signature proves control of a key at message-signing time. It does not prove that an evidence claim is accurate, that the named organization controls the key, or that the submitted source is authentic.

## Confidentiality and privacy

OIX v0.1 signs messages but does not encrypt their payloads. Do not send restricted case details, private prompts, secrets, personal data, or private internal queries over an OIX transport until an independently reviewed confidentiality layer and key-management procedure exist.

The event classification label is not an egress control. A future export gateway must enforce explicit allowlists and redaction before any event leaves the local trust boundary.

Private model reasoning or hidden chain-of-thought is not an exchange format. Share concise, authorized conclusions, methods, evidence references, uncertainty, and reproducible artifacts instead.

## Delivery and recovery

The network adapter must use durable replay protection and an inbox/outbox pattern. A received message must not be acknowledged to its transport until the local durable write succeeds. Consumers must be idempotent because delivery is at least once.

The current SQLite event journal is single-host. It is not the shared multi-host replay database or distributed message broker. A later transport or storage backend must pass the same conformance suite and document its ordering, retention, retry, and failure semantics.

## Required conformance tests

- Trusted and untrusted sender cards.
- Valid, malformed, expired, future-dated, oversized, and tampered envelopes.
- Replayed (sender, nonce) pairs across process restarts.
- Recipient mismatch and unsupported protocol version.
- Unknown message types and malformed payload schemas.
- Capability denial despite a valid signature.
- Approval-required task remains unclaimable before local approval.
- Crash after local write but before transport acknowledgement.
- Duplicate delivery does not create duplicate internal events or tasks.
- Evidence provenance survives normalization without promoting unverified evidence to fact.

See the OIX v0.1 protocol and OIX threat model:
- https://github.com/HuffmanMainNode/Open-Intelligence-Exchange/blob/main/protocol/oix-v0.1.md
- https://github.com/HuffmanMainNode/Open-Intelligence-Exchange/blob/main/docs/THREAT_MODEL.md
