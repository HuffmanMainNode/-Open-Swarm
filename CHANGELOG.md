# Changelog

All notable changes to Open-Swarm are recorded here. This project is experimental; no stable release has been declared.

## 0.1.0 - Unreleased

- Add a local-first SQLite task queue, audit trail, worker registry, mock adapter, and optional Ollama adapter.
- Add bounded retries, lease ownership checks, stale-worker protection, and legacy queue migration.
- Add human approval and rejection workflow with audited CLI commands.
- Restrict Ollama URL schemes and plain HTTP to loopback.
- Add POSIX state directory/database permissions, tests, and Python 3.11-3.13 CI.
- Document plaintext queue storage, policy-label limitations, and the absence of OS-level sandboxing.
