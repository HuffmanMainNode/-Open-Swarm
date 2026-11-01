# Open-Swarm

Provider-independent, local-first orchestration for cooperative AI agents.

Open-Swarm is a Python runtime for assigning bounded tasks to specialist workers, selecting local or custom model adapters, enforcing capability and approval policies, and recording an auditable event trail. The core requires no hosted model provider, account, API key, or token-metered service.

Status: initial implementation. The deterministic mock adapter supports tests and demos. The optional Ollama adapter requires a separately running local Ollama service. This is an orchestration prototype, not a production security boundary.

## Quick start

    python -m venv .venv
    . .venv/bin/activate
    pip install -e '.[dev]'
    openswarm demo
    pytest

## Included

- SQLite task queue with durable state, leases, retries, and bounded capacity.
- Worker registry with explicit capabilities.
- Pluggable model adapters, mock and local Ollama.
- Policy gates, human approval callback, output limits.
- Structured audit events.
- CLI, tests, and GitHub Actions CI.

## Documentation

- docs/ARCHITECTURE.md
- docs/SECURITY.md
- docs/OIX.md
- CONTRIBUTING.md

No cloud fallback is attempted by the local adapter.
