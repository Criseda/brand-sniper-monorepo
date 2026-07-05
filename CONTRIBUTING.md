# Contributing

## Development workflow

1. **Create a feature branch** from `main`:
   ```bash
   git checkout main
   git pull
   git checkout -b feat/my-feature
   ```

2. **Make your changes** — follow the conventions in [.agents/AGENTS.md](.agents/AGENTS.md):
   - Use prefix-based structured logging, not `print()`
   - No emojis in source code, logs, or comments
   - Keep shared models in `packages/shared_utils/src/shared_utils/models.py`
   - Use `contextvars.ContextVar` for thread-safe telemetry

3. **Run quality checks locally**:
   ```bash
   uv sync --all-packages --group dev
   uv run ruff check
   uv run ruff format --check
   uv run mypy apps/backend/ apps/listener/ apps/analytics/
   uv run pytest
   ```
   Fix any issues before committing.

4. **Commit your changes** with a conventional commit message:
   ```
   feat: add new endpoint for ...
   fix: resolve crash when ...
   refactor: extract ... into ...
   docs: update README ...
   ```

5. **Push and open a pull request** against `main`:
   ```bash
   git push -u origin feat/my-feature
   ```
   Then open a PR on GitHub. Use the [pull request template](.github/pull_request_template.md).

6. **CI checks** run automatically on your PR. All checks must pass before merging. The `main` branch is protected — direct pushes are not allowed.

## Branch naming

Use a short prefix followed by a description:

| Prefix       | Use case                        |
|--------------|---------------------------------|
| `feat/`      | New features                    |
| `fix/`       | Bug fixes                       |
| `refactor/`  | Code restructuring              |
| `docs/`      | Documentation changes           |
| `ci/`        | CI/CD pipeline changes          |
| `chore/`     | Maintenance, deps, tooling      |

## Code quality expectations

- **Lint**: `ruff check` must pass with zero errors (configured in `pyproject.toml`)
- **Format**: `ruff format` must pass (`--check` mode in CI)
- **Types**: `mypy` must pass with no new errors; existing error suppressions in `mypy.ini` are acceptable
- **Tests**: all existing tests must pass; new code should include tests
- **Logging**: use the `logging` module with prefix-based format strings (`[ANOMALY]`, `[PAPER TRADE]`, `[CFO]`, etc.)

## Need help?

Check the [AGENTS.md](.agents/AGENTS.md) for a quick reference of available commands, package structure, and testing quirks.
