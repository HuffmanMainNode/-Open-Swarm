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
