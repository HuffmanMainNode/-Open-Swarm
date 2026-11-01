# Architecture

Open-Swarm is a small orchestration kernel, not a monolithic autonomous agent.

## Execution flow

1. A caller submits a typed task to the SQLite queue.
2. The runtime claims one queued task and records the transition.
3. A named worker is resolved from the registry.
4. Policy checks adapter allowlisting, prompt bounds, requested capabilities, denied markers, and approval requirements.
5. The selected adapter generates output.
6. Output bounds are checked and the task is marked succeeded, blocked, requeued, or failed.
7. Structured lifecycle events are persisted in SQLite.

## Components

- models: task and audit data structures.
- queue: durable task and audit persistence.
- policy: fail-closed application-level gates.
- runtime: worker selection, policy evaluation, adapter execution.
- adapters: local or custom inference implementations.
- cli: operator-facing entry point.

## Execution model

This initial release is single-process and executes one task per run-once call. It does not claim distributed scheduling, exactly-once execution, or cluster coordination. SQLite lease recovery is intended for a single host.
