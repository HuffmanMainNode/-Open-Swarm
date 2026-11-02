# Local event journal contract

**Status:** Phase 0 foundation. The event schema is an integration contract; this implementation is a durable, single-host SQLite journal, not a distributed message broker.

## Why this exists

The four open-source intelligence components need a common way to publish observations and consume them without importing one another's internal database schemas. This journal provides the first shared contract. It is designed for local-first deployments and can later be implemented by other storage or transport backends without changing the event vocabulary.

## Event envelope

Every event contains:

- `event_id`: UUID used for idempotency.
- `event_type`: namespaced lowercase event name, for example `osint.indicator.observed`.
- `schema_version`: MAJOR.MINOR contract version.
- `occurred_at`: timezone-aware RFC 3339 timestamp.
- `source`: component or collector identifier.
- `classification`: `public`, `internal`, or `restricted`.
- `payload`: bounded JSON object containing the event data.
- `evidence_refs`: optional content-addressed or source references.
- `correlation_id` and `causation_id`: optional incident and parent-event linkage.

Events are append-only. Reusing an event ID with identical content is idempotent; reusing it with different content is rejected. The implementation accepts only deterministic JSON values: null, booleans, strings, integers, arrays, and objects with string keys. Floating-point values are rejected; represent fractional measurements as decimal strings or scaled integers. Payload nesting is limited to 32 levels, arrays and objects to 10,000 members each, and the complete event to 64 KiB.

## Delivery semantics

- A consumer reads events after its last acknowledged sequence.
- Reading does not advance its durable offset.
- The consumer acknowledges only after successful processing.
- A crash before acknowledgement causes redelivery: delivery is **at least once**.
- Consumers must be idempotent.
- Consumer IDs have independent offsets.
- Use one active poller per consumer ID.
- The SQLite implementation is for one host or a supported shared local volume. It is not a multi-host coordination service.

## Open-Swarm transactional outbox

Open-Swarm now writes each task lifecycle audit event and its matching outbox row in the same SQLite transaction as the task-state change. A relay can publish committed outbox rows into the event journal and then mark them delivered. If the process stops after publication but before marking the row, the next attempt republishes the same event ID; the journal deduplicates it. This is at-least-once delivery, not exactly-once execution.

The current relay is an explicit callable, not a continuously running background service. It must be supervised by a future local service or operator workflow. Use one relay process per task database. The outbox intentionally does not include the task prompt or model result body. Audit details may still contain diagnostic error text, so these events remain classified as internal and must not be exported without a separate review and redaction policy.

## Data protection and OSINT integrity

The journal does not make private data public. Producers must classify events before publication and must not put credentials, private keys, private prompts, unredacted personal data, or internal case notes into public events. Restricted payloads remain local and require a future explicit export policy before crossing a process or network trust boundary.

External source content is untrusted evidence, even when digitally signed by its publisher. Preserve source URI, retrieval time, content hash, collection method, and limitations in the payload or referenced evidence record. Do not promote an observation to a verified fact merely because multiple agents repeat it.

The event journal stores JSON in SQLite. File permissions are best-effort local protections, not encryption, access control, or a hardened security boundary. Protect backups and host access. Encryption at rest and authenticated multi-user access are future deployment requirements.

## Current limits

This phase does not provide network transport, event subscriptions over sockets, a scheduler, a distributed broker, cross-repository adapters, exactly-once processing, consensus, or automatic incident response. It is the local durable journal and versioned event contract on which those later features can be built and tested.

## Example

```python
from openswarm.events import PlatformEvent, SQLiteEventBus

bus = SQLiteEventBus(".openswarm/events.sqlite3")
receipt = bus.publish(PlatformEvent(
    event_type="osint.indicator.observed",
    source="halloween-osint",
    classification="public",
    payload={"indicator_type": "domain", "indicator": "example.org"},
    evidence_refs=["sha256:" + "a" * 64],
))

for record in bus.read("cyber-observatory", limit=100):
    # Validate and process the event idempotently.
    process(record.event)
    bus.acknowledge("cyber-observatory", record.sequence)
```

## Planned conformance

Before a second backend is introduced, it must pass the same contract tests for idempotent publication, ordering, independent consumer offsets, redelivery, invalid event rejection, and acknowledgement safety. A future broker adapter must document its ordering and delivery guarantees rather than inheriting claims from this SQLite implementation.
