# Open Intelligence Exchange integration

Open-Swarm is designed to interoperate with the separate Open-Intelligence-Exchange project without coupling the task engine to a transport.

## Current state

This release does not yet ship an OIX network client and does not claim wire-level interoperability. A future bridge should translate OIX-validated envelopes into task submissions and publish lifecycle events.

## Bridge requirements

- Validate envelope signature and canonical serialization before accepting a message.
- Enforce replay protection with OIX durable replay storage.
- Map sender identity to an explicit worker identity and capability set.
- Reject unknown message types and oversized payloads.
- Preserve envelope hash and sender metadata in the audit record.
- Never treat an incoming message as approval for privileged actions.
- Keep transport credentials outside prompts and model context.

See https://github.com/HuffmanMainNode/Open-Intelligence-Exchange.
