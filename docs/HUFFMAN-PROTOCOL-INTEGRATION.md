# Huffman Protocol cross-project integration

## Purpose

This suite runs the real OIX, Open-Swarm, and HDI Python packages together against shared local SQLite state. It validates lifecycle behavior across project boundaries and emits a signed Huffman assurance receipt for the observed test run.

It is deliberately separate from Open-Swarm's ordinary unit tests. The core package remains independently installable and does not acquire runtime dependencies on OIX or HDI.

## Scenarios

The initial integration test covers three real package interactions:

1. **Revocation before dispatch:** OIX accepts and approves a signed request, then revocation terminally invalidates it. Open-Swarm cannot hand it off, and restoring trust does not revive the old approval.
2. **Revocation during inference:** OIX revokes the peer while the Open-Swarm adapter is running. Open-Swarm marks the task BLOCKED, discards the generated output, and does not create a result outbox entry.
3. **Revocation after result staging:** Open-Swarm completes a task and stages a result for operator approval. OIX revocation changes the undelivered result to rejected, preventing later signing or delivery.

A deterministic adapter is used only to exercise the protocol lifecycle. This is not a model-quality or autonomous-reasoning benchmark.

## Run locally

Clone the three repositories and check out the same pinned revisions used by the integration workflow:

- OIX: 2637d90ba8ba3e21d4841109312384650fbe8529
- HDI: 37dccbf498f589b356162dd8046d7b57b638c1af
- Open-Swarm: the branch or commit under test

From the Open-Swarm repository root:

    python -m pip install -e ../Open-Intelligence-Exchange
    python -m pip install -e ../Huffman-Defensive-Intelligence
    python -m pip install -e ".[dev]"
    python -m pytest -q integration_tests
    ruff check src integration_tests
    bandit -q -r src

The integration workflow performs these steps using pinned dependency commits.

## Assurance receipt

After the scenarios pass, the test creates an Ed25519-signed Huffman assurance receipt. Its suite digest binds the test source and the exact OIX, Open-Swarm, and HDI revisions. Each result includes a digest of the observed SQLite state.

In CI, receipts are uploaded as a workflow artifact. The verifier key is generated for the test run, so the artifact proves integrity relative to that ephemeral key but does not establish an externally trusted verifier identity. The receipt is evidence of this bounded test run, not a certification or proof that an entire deployment is secure.

## Boundaries

- The integration uses one local SQLite database shared by the three packages.
- It is not a multi-host or WAN deployment test.
- It does not implement AAP, A2A, or MCP adapters.
- It does not interrupt model inference already in progress; it discards its result if revocation is observed at completion.
- Future adapters that perform external side effects must check authorization immediately before each effect.
