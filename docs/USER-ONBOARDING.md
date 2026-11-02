# Human onboarding

Open-Swarm is a local-first task queue and worker runtime. The default mock adapter is for safe tests and demonstrations. An optional Ollama adapter uses a separately installed local Ollama server. No hosted model or API key is required by the core.

## Install

Python 3.11+:

```bash
git clone https://github.com/HuffmanMainNode/-Open-Swarm.git
cd -Open-Swarm
python -m venv .venv
# Linux/macOS
source .venv/bin/activate
# Windows PowerShell
.venv\\Scripts\\Activate.ps1
python -m pip install -e ".[dev]"
```

## Run the safe local demo

```bash
openswarm demo
openswarm tasks
openswarm audit --limit 20
pytest -q
ruff check .
```

The demo uses the deterministic mock worker. It does not contact a model provider.

## Submit a task

```bash
openswarm submit --worker general --adapter mock --task "Summarize the public project README"
openswarm run-once
openswarm tasks
```

For work requiring explicit review:

```bash
openswarm submit --worker general --task "Prepare an evidence summary" --requires-approval
openswarm tasks --status pending_approval
openswarm approve TASK_ID
openswarm run-once
```

Replace `TASK_ID` with the identifier returned by the task listing. Reject instead with `openswarm reject TASK_ID`.

## Optional local model

Install and start Ollama separately, then pull a model appropriate for your hardware:

```bash
openswarm submit --worker general --adapter ollama --model qwen2.5:3b --task "Explain DNS in plain language"
openswarm run-once
```

No cloud fallback is attempted. Model availability, memory requirements and performance depend on the local machine.

## Next steps

- [Agent onboarding](AGENT-ONBOARDING.md)
- [Architecture](ARCHITECTURE.md)
- [Security model](SECURITY.md)
- [OIX integration](OIX.md)
- [Outbox relay operations](OUTBOX-RELAY.md)

Open-Swarm is an orchestration prototype, not an OS-level security sandbox or production distributed scheduler.
