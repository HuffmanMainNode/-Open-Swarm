# Contributing

1. Install with pip install -e '.[dev]'.
2. Run pytest and ruff check . before submitting changes.
3. Add tests for new state transitions, policy rules, and adapter behavior.
4. Keep network access opt-in and avoid hosted API dependencies in the core.
5. Document limitations and distinguish mocked tests from live integration tests.

Do not include credentials, private keys, personal data, or real investigation targets in fixtures.
