# Contributing to dbt-duckdb

Thanks for your interest in contributing to dbt-duckdb! This guide covers everything you need to get started.

## Development Setup

**Requirements:** Python 3.10+

```bash
# Clone the repo
git clone https://github.com/duckdb/dbt-duckdb.git
cd dbt-duckdb

# Create and activate a virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install the package in editable mode with dev dependencies
pip install -e .
pip install -r dev-requirements.txt

# Install pre-commit hooks
pre-commit install
```

### Using the Dev Container

The repo includes a [dev container](.devcontainer/) configuration for VS Code / GitHub Codespaces. Open the project in VS Code and select **Reopen in Container** (or launch a Codespace on GitHub). The container automatically runs `pip install -e . && pip install -r dev-requirements.txt` on creation.

## Running Tests

Tests use [pytest](https://docs.pytest.org/) and [tox](https://tox.wiki/). The test suite is organized into unit tests and several functional test profiles.

```bash
# Run unit tests directly
pytest tests/unit

# Run unit tests via tox (across Python versions)
tox -e unit

# Run functional adapter tests
tox -e functional

# Run functional tests with file-based DBs
tox -e filebased

# Run plugin tests
tox -e plugins

# Run fsspec tests
tox -e fsspec
```

Some test suites require additional setup:

- **MotherDuck tests** (`tox -e md`) require a MotherDuck token.
- **Buena Vista tests** (`tox -e buenavista`) require a running BV server.
- Tests marked `with_s3_creds` require AWS credentials.
- Tests marked `requires_ducklake` require the DuckLake extension.

## Code Style

Code style is enforced automatically via [pre-commit](https://pre-commit.com/) hooks:

- **[Ruff](https://docs.astral.sh/ruff/)** for linting and formatting (line length: 99)
- **[reorder-python-imports](https://github.com/asottile/reorder_python_imports)** for import ordering
- **[mypy](https://mypy-lang.org/)** for type checking (`dbt/adapters/` files)
- Standard pre-commit checks: trailing whitespace, end-of-file fixer, YAML/JSON validation

Run the hooks manually against all files:

```bash
pre-commit run --all-files
```

## Project Structure

```
dbt/adapters/duckdb/
├── connections.py     # Connection management
├── credentials.py     # Profile/credential handling
├── environments/      # Local and remote (Buena Vista) environments
├── plugins/           # Plugin system (Excel, GSheet, Iceberg, Delta, etc.)
├── secrets.py         # Secret management for S3, GCS, Azure, etc.
├── impl.py            # Core adapter implementation
├── relation.py        # Relation (table/view) handling
└── utils.py           # Shared utilities

tests/
├── unit/              # Unit tests
└── functional/
    ├── adapter/       # Core adapter functional tests
    ├── fsspec/        # fsspec integration tests
    └── plugins/       # Plugin-specific tests
```

## Pull Request Process

1. Fork the repo and create a branch from `master`.
2. Make your changes. Add or update tests for any new behavior.
3. Ensure `pre-commit run --all-files` passes.
4. Run the relevant test suite(s) to confirm nothing is broken.
5. Open a PR against `master` with a clear description of the change.

For bug fixes, include steps to reproduce or a failing test that your fix resolves. For new features, explain the use case and any configuration changes.
