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
- Worker registry with explicit capabilities and maximum concurrency.
- Pluggable model adapter interface, deterministic mock adapter, and local Ollama HTTP adapter.
- Policy engine for capability checks, output limits, task allow/deny rules, and human-approval gates.
- Structured audit events stored in SQLite.
- CLI for submitting, listing, running, and inspecting tasks.
- Tests and GitHub Actions CI.

## Design principles

1. **Local-first:** no hosted inference provider is required by the core.
2. **Least privilege:** workers receive only their declared capabilities.
3. **Bounded execution:** queue size, worker count, task duration, retries, and output size are configurable.
4. **Human control:** approval-required tasks are not run automatically.
5. **Observable:** task transitions and policy decisions are auditable.
6. **Replaceable:** model adapters can be swapped without changing orchestration.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Security model](docs/SECURITY.md)
- [OIX integration](docs/OIX.md)
- [Development](CONTRIBUTING.md)

## Example

```bash
openswarm submit --worker general --task "Summarize the public README in three bullets"
openswarm run-once
openswarm tasks
openswarm audit --limit 20
```

To use a local Ollama model:

```bash
openswarm submit --worker general --adapter ollama --model qwen2.5:3b --task "Explain DNS in plain language"
openswarm run-once
```

The Ollama server must be running locally. No cloud fallback is attempted.

## License

MIT. See [LICENSE](LICENSE).
