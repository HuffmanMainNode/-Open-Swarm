# Security Policy

## Supported scope

Open-Swarm is an experimental local orchestration prototype. No stable release series or response-time guarantee has been declared. The latest default branch is the supported development target.

## Reporting a vulnerability

Do not publish exploitable details, credentials, private keys, task prompts containing sensitive information, or personal data in a public issue.

Use GitHub's private vulnerability reporting for this repository if enabled. If it is unavailable, contact the repository owner through the verified GitHub profile: https://github.com/HuffmanMainNode

Include the affected commit, impact, prerequisites, safe reproduction steps, and suggested mitigation. Do not test systems or adapters you do not own or have explicit permission to assess.

## Security limitations

- Queue prompts, results, errors, and audit details are stored in plaintext SQLite.
- POSIX file modes reduce local exposure but do not encrypt databases or backups.
- Prompt-marker checks are not prompt-injection protection.
- Capability labels are checked by runtime policy but are not OS-level permission grants.
- The runtime does not provide process isolation, a hardened sandbox, distributed coordination, or multi-tenant isolation.
- Passing tests are not a security certification or independent audit.
