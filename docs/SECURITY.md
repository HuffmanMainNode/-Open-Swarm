# Security model and limitations

## Included controls

- Bounded prompt and output lengths.
- Bounded queue capacity and lease duration.
- Explicit worker names and capability declarations.
- Adapter allowlisting.
- Approval-required tasks enter a durable `pending_approval` state and are not claimable until an operator explicitly approves them. Approve/reject transitions are audited.
- Localhost-only plain HTTP for bundled Ollama; remote Ollama endpoints require HTTPS.
- Structured SQLite audit events for submissions, claims, lease expiry, and terminal states.
- No shell, filesystem, browser, network, or arbitrary tool execution is granted to model workers by this runtime.

## Local state and data handling

The SQLite queue stores task prompts, results, errors, and audit details in plaintext. On POSIX systems, the default `.openswarm` directory is created with mode `0700` and the database file with mode `0600`. These permissions do not encrypt the database or protect backups, snapshots, or data copied to another filesystem. Do not submit passwords, private keys, access tokens, or personal data as task prompts.

## Limitations

The denied prompt marker check is a narrow filter, not a prompt-injection defense. Worker capability names are labels checked by the runtime against a worker declaration; they are not permission grants enforced inside adapters. A custom or compromised adapter can still use the host process privileges available to it.

These are application-level guardrails, not a hardened sandbox. The runtime does not execute generated code, isolate processes, provide multi-tenant security, or prevent a compromised adapter from using host process privileges. Model output can still be misleading or unsafe.

Before production use, add OS-level sandboxing, authenticated operator identity, secret management, signed audit exports, migrations, concurrency tests, rate limits, dependency scanning, and independent security review.
