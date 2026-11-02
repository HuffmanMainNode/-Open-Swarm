# Continuous ingestion supervisor

Open-Swarm now provides a checkpointed polling supervisor in `openswarm.ingestion`. Sources are explicit Python adapters registered by the operator. The supervisor persists each source's checkpoint, polling interval, lease, last success, failure count, and next due time in the same SQLite database as the event journal.

## Guarantees

- A source poll returns a bounded `SourceBatch` containing events and the checkpoint after those events.
- Event publication and checkpoint advancement occur in one `BEGIN IMMEDIATE` transaction.
- A validation or database failure rolls back the entire batch and leaves the prior checkpoint intact.
- Event IDs are idempotent. Replayed source items do not create duplicate journal rows when their canonical content is unchanged.
- A per-source SQLite lease prevents two local supervisor processes from polling the same source concurrently. Expired leases can be recovered after a process crash.
- Failures are recorded by exception class only. Retry delay increases exponentially and is capped.
- Batch size is bounded to 1,000 events; checkpoint strings are bounded to 4,096 characters.
- Source adapters are code-owned and registered explicitly. The supervisor does not dynamically load arbitrary URLs, code, or remote instructions.

## CISA Known Exploited Vulnerabilities source

The optional `openswarm.adapters.cisa_kev.CisaKevSource` adapter uses Huffman Cyber Observatory's existing fixed-endpoint CISA KEV client. Install Huffman Cyber Observatory alongside Open-Swarm to enable the default live fetcher.

Example:

```python
from openswarm.adapters.cisa_kev import CisaKevSource
from openswarm.events import SQLiteEventBus
from openswarm.ingestion import IngestionSupervisor

bus = SQLiteEventBus(".openswarm/events.sqlite3")
supervisor = IngestionSupervisor(bus)
supervisor.register_source(
    "hco.cisa-kev",
    CisaKevSource(),
    interval_seconds=300,
)
supervisor.run_forever()
```

The first poll establishes a baseline and does not flood the event journal with the entire historical catalog. Later feed changes emit records added on or after the last observed date. Same-day replays are harmless because event IDs are deterministic. Set `CisaKevSource(initial_backfill=True)` to ingest the full current catalog in bounded batches.

Each emitted event is `intelligence.vulnerability.kev_added` and includes the CVE identifier, vendor/product, date added, required action, due date, source URL, and source feed digest. Events are classified `internal`; downstream systems must not treat a KEV record as proof that a specific local asset is affected.

Run the synthetic demonstration with `python examples/cisa_kev_ingestion_demo.py`. It uses an injected fixture and makes no network request.

## Operational limits

- This is periodic polling, not a push stream or guaranteed real-time feed. The example interval is five minutes; the operator controls the cadence.
- The supervisor and journal are single-host SQLite components. This is not a distributed broker or multi-host scheduler.
- Each adapter must enforce its own network timeouts and source-specific validation.
- The CISA feed is a public source snapshot. A clean or unchanged feed is not proof that a system is secure.
- Initial historical backfill is opt-in because it can generate a large number of events.
- The supervisor does not perform vulnerability matching against local assets, execute remediation, or trigger consequential actions.

For current state, call `supervisor.status()`. It reports checkpoints, due times, lease state, last success, and error classes without exposing exception messages or private source details.
