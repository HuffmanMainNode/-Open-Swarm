# Security model and limitations

## Included controls

- Bounded prompt and output lengths.
- Bounded queue capacity and lease duration.
- Explicit worker names and capability declarations.
- Adapter allowlisting.
- Human-approval callback for tasks marked approval-required. The default callback denies.
- Localhost-only plain HTTP for bundled Ollama; remote Ollama endpoints require HTTPS.
- Structured SQLite audit events for submissions, claims, lease expiry, and terminal states.
- No shell, filesystem, browser, network, or arbitrary tool execution is granted to model workers by this runtime.

## Limitations

These are application-level guardrails, not a hardened sandbox. The runtime does not execute generated code, isolate processes, provide multi-tenant security, or prevent a compromised adapter from using host process privileges. Model output can still be misleading or unsafe.

Before production use, add OS-level sandboxing, authenticated operator identity, secret management, signed audit exports, migrations, concurrency tests, rate limits, dependency scanning, and independent security review.
