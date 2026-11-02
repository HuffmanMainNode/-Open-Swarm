# Open-Swarm

Provider-independent, local-first orchestration for cooperative AI agents.

Open-Swarm is a small Python runtime for assigning bounded tasks to specialist workers, selecting local or custom model adapters, enforcing capability and approval policies, and recording an auditable event trail. The core has no hosted model dependency, account requirement, API key, or token-metered service.

> **Status:** initial implementation. The deterministic mock adapter is intended for tests and demos. The optional Ollama adapter requires a separately installed and running Ollama server. This project is an orchestration prototype, not a production security boundary.

## Quick start

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e '.[dev]'
openswarm demo
pytest
```

## Included capabilities

- SQLite task queue with durable state, leases, retries, and bounded queue capacity.
- Worker registry with declared capability labels; the current runtime executes one task per run-once call.
- Pluggable model adapter interface, deterministic mock adapter, and local Ollama HTTP adapter.
- Policy engine for capability checks, output limits, task allow/deny rules, and human-approval gates.
- Structured audit events stored in SQLite.
- Transactional task-lifecycle outbox and a local SQLite event journal with at-least-once relay semantics.
- Optional OIX inbox adapter for atomic, idempotent handoff of operator-approved remote tasks.
- CLI for submitting, approving, rejecting, listing, running, and inspecting tasks.
- Tests and GitHub Actions CI.

## Design principles

1. **Local-first:** no hosted inference provider is required by the core.
2. **Policy checks:** requested capability labels are compared with worker declarations; adapters do not enforce OS-level grants.
3. **Bounded execution:** queue capacity, lease duration, retry count, prompt size, and output size have explicit limits.
4. **Human control:** approval-required tasks are not run automatically.
5. **Observable:** task transitions and policy decisions are auditable.
6. **Replaceable:** model adapters can be swapped without changing orchestration.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Security model](docs/SECURITY.md)
- [OIX integration](docs/OIX.md)
- [OIX inbox atomic handoff adapter](docs/OIX-INBOX-ADAPTER.md)
- [Local OIX-to-Open-Swarm end-to-end demo](examples/oix_handoff_demo.py)
- [Local event journal contract](docs/EVENT-BUS.md)
- [Supervised outbox relay operations](docs/OUTBOX-RELAY.md)
- [OIX-to-platform event mapping](docs/OIX-MAPPING.md)
- [Development](CONTRIBUTING.md)

## Example

```bash
openswarm submit --worker general --task "Summarize the public README in three bullets"
openswarm run-once
openswarm tasks
openswarm audit --limit 20
openswarm relay-status
openswarm relay-once
```

To use a local Ollama model:

```bash
openswarm submit --worker general --adapter ollama --model qwen2.5:3b --task "Explain DNS in plain language"
openswarm run-once
```

The Ollama server must be running locally. No cloud fallback is attempted.

## Local integration relay

The transactional task outbox can be relayed into the local event journal without a hosted broker:

```bash
openswarm relay-status
openswarm relay-once
openswarm relay --poll-interval 2 --batch-size 100
```

The continuous relay runs in the foreground and stops with Ctrl+C. Use one relay process per task database. See [relay operations](docs/OUTBOX-RELAY.md) for delivery guarantees, recovery behavior, and security limitations.

For approval-required tasks, submission places the task in `pending_approval`; it is not eligible for execution until an operator runs `openswarm approve <task-id>`. Use `openswarm reject <task-id>` to reject it. Approval and rejection are recorded in the audit log. The optional OIX adapter accepts only tasks already approved in the OIX inbox, then commits the Open-Swarm task, audit/outbox events, durable remote-message mapping, and OIX dispatched state in one shared SQLite transaction. Run `openswarm --db .openswarm/state.sqlite3 oix-handoff` against the same database configured in OIX. See [adapter operations](docs/OIX-INBOX-ADAPTER.md).

## License

MIT. See [LICENSE](LICENSE).


## OIX result return staging

For tasks handed off from OIX, terminal task completion now creates a durable `oix_result_outbox` record in the same SQLite transaction as task state and audit. Results remain `pending_approval`; Open-Swarm does not sign or transmit them. The OIX repository's `SQLiteResultPublisher` handles explicit approval, signing, and persistence of the exact `task.result` envelope for a future transport.

See [OIX inbox and result staging](docs/OIX-INBOX-ADAPTER.md) and the local cross-repository demonstration in `examples/oix_result_demo.py`. The demonstration verifies a signed return envelope but does not claim remote delivery. OIX v0.1 does not encrypt payloads.


Phase 4 interoperability adds an encrypted OIX request/result demonstration in `examples/oix_secure_result_demo.py`. It verifies the signed request after decryption, runs the local Open-Swarm mock worker, encrypts the approved signed result to the original peer's certified X25519 key, and verifies the return locally. No network transport or remote delivery is claimed.


Phase 5 interoperability extends the encrypted round trip with a durable receiver inbox and a signed OIX `delivery.receipt` bound to the exact encrypted result wrapper. The sender records delivery only after verifying the peer signature, original message ID, recipient route, and ciphertext digest. The demonstration remains local-only and does not claim network delivery.


Phase 6 interoperability exercises signed one-time-prekey bundles in both directions. Each peer keeps its private prekeys locally; successful authenticated decryption consumes the matching key. The end-to-end example verifies the task handoff, encrypted result return, durable peer inbox acceptance, signed receipt, and retry reuse without opening network transport.


The Phase 7 interoperability workflow checks out OIX's atomic prekey/inbox acceptance implementation. Each receiver now commits message acceptance and one-time-key consumption in a single local SQLite transaction; inbox failure rolls both back.


### Mutual-TLS interoperability demonstration

With OIX Phase 8 installed, run `python examples/oix_mutual_tls_e2e_demo.py` to exercise batches of encrypted task requests and results over persistent mutual-TLS loopback connections between two independent OIX identities and databases, with certificate URI SANs bound to each agent fingerprint. The demo requires explicit inbox approval, uses the Open-Swarm mock worker, verifies signed delivery receipts in both directions, and reports that both agents run on one physical host. It is a network integration test, not a WAN deployment claim.


### OIX evidence ingestion

With OIX Phase 11 installed, `OIXEvidenceIngestor` can transfer an operator-approved `evidence.submit` record into the local Open-Swarm event journal. The OIX inbox and event journal must share one SQLite file so the event append and inbox status transition are atomic. See [OIX Inbox Adapter](docs/OIX-INBOX-ADAPTER.md) for security boundaries and the persistent mutual-TLS evidence-stream demonstration.


### Four-repository OSINT evidence exchange

The optional bridges in Halloween OSINT and Huffman Cyber Observatory can sign local artifacts as OIX evidence records. The Open-Swarm interoperability workflow validates both sources through one persistent mutual-TLS exchange, applies separate operator approvals, and atomically ingests evidence into its event journal. See [OIX Inbox Adapter](docs/OIX-INBOX-ADAPTER.md) and run `python examples/osint_ecosystem_e2e_demo.py` in the cross-repository environment.
