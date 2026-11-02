# Supervised local outbox relay

**Status:** Phase 1 implementation. This is a local single-host service, not a distributed broker.

## Commands

Use the same task database path for task operations and relay operations:

```bash
# Inspect queue and relay health
openswarm --db .openswarm/state.sqlite3 relay-status

# Publish one batch and exit
openswarm --db .openswarm/state.sqlite3 relay-once --events-db .openswarm/events.sqlite3

# Run continuously until Ctrl+C
openswarm --db .openswarm/state.sqlite3 relay --events-db .openswarm/events.sqlite3 --poll-interval 2 --batch-size 100
```

The relay is intentionally a foreground process. A service manager may supervise it, but daemonization and platform-specific service installation are not part of this increment. The command handles Ctrl+C and exits cleanly.

## Delivery and recovery

1. Task state, audit entry, and outbox record are committed together by Open-Swarm.
2. The relay obtains a SQLite lease for the task database before processing.
3. Each pending record increments its attempt count.
4. The relay publishes the stable event ID to the local event journal.
5. Only after successful publication does it mark the outbox record published.
6. If publication succeeds but marking fails, the next run republishes the same event ID. The event journal recognizes the duplicate and prevents a second event record.
7. If the process stops before publication, the outbox record remains pending for the next run.

This is **at-least-once delivery with idempotent journal insertion**, not exactly-once task execution. Consumers still need idempotent processing and explicit acknowledgements.

## Single-instance coordination

A SQLite lease row prevents two relay processes from intentionally processing the same task database at once. The lease is renewed before each event and released on normal or exceptional exit. If a process is killed, its lease expires after the configured interval and another process can recover. The lease is local database coordination and must not be treated as a distributed lock across unreliable network filesystems.

Run one relay service for a given task database. Multiple independent installations should use separate local databases and later communicate through an authenticated transport.

## Health output

`relay-status` reports:
- pending and published outbox record counts;
- total publication attempts;
- age of the oldest pending record;
- whether the relay lease currently appears active.

The status output deliberately excludes task prompts, model outputs, evidence contents, and diagnostic payloads. Service logs report error classes rather than exception text to reduce accidental disclosure.

## Failure policy

- A failed publication leaves the outbox row pending.
- The attempt counter is incremented before publication.
- The continuous runner retries on its next polling interval.
- The relay does not silently delete or dead-letter events.
- A live competing lease is treated as a configuration/process conflict and causes that relay process to stop.
- Operators should inspect local logs and status before taking manual recovery action.

There is no exponential backoff or dead-letter queue in this increment. Polling interval is configurable and provides a simple bounded retry cadence. More advanced retry scheduling, stale-event alerts, retention, and encrypted storage require separate design and tests.

## Security boundaries

The relay is not a confidentiality layer. The event journal remains unencrypted SQLite storage. The current outbox event payloads are internal-classified and omit task prompts and result bodies, but audit details may contain diagnostic information. Do not export these events over a network or to a public consumer without an explicit classification, redaction, authentication, authorization, and encryption policy.

## Validation

The test suite exercises successful delivery, publication-before-mark crash recovery, duplicate suppression, active-lease exclusion, and status redaction. CI must pass on the supported Python matrix before this increment is considered accepted.
