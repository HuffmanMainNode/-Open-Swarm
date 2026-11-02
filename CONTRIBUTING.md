# Contributing

1. Install the project with `python -m pip install -e ".[dev]"`.
2. Run `python -m pytest -q`, `ruff check .`, and `bandit -q -r src`.
3. Add regression tests for every queue transition, policy decision, and adapter security rule.
4. Keep network access explicit and avoid hosted API dependencies in the core.
5. Document limitations and distinguish mocked tests from live integrations.

Never commit credentials, private keys, personal data, local SQLite databases, or real investigation targets.
