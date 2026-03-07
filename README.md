# dbt-duckdb

The [dbt](https://getdbt.com) adapter for [DuckDB](https://duckdb.org). Build fast, local, and cloud-native data pipelines with SQL and Python.

**[Full documentation](https://duckdb.github.io/dbt-duckdb)**

## Installation

```bash
pip install dbt-duckdb
```

Requires Python >= 3.10, dbt-core >= 1.8, and DuckDB >= 1.1.

## Development setup

```bash
git clone https://github.com/duckdb/dbt-duckdb.git
cd dbt-duckdb

pip install -e ".[glue,docs]"
pip install -r dev-requirements.txt

pre-commit install
```

A dev container is also available — open the repo in VS Code and select **Reopen in Container**.

## Running tests

Tests are managed with [tox](https://tox.wiki):

```bash
tox -e unit          # unit tests
tox -e functional    # functional tests
tox -e filebased     # file-based functional tests
tox -e fsspec        # fsspec filesystem tests
tox -e plugins       # plugin tests
tox -e buenavista    # BuenaVista server tests
tox -e md            # MotherDuck tests (requires MOTHERDUCK_TOKEN)
```

## Code quality

[pre-commit](https://pre-commit.com) runs [ruff](https://docs.astral.sh/ruff/) (lint + format) and [mypy](https://mypy-lang.org/) on every commit:

```bash
pre-commit run --all-files
```

## Building the docs locally

```bash
zensical build --clean   # output in /site
```

## Hosting the docs locally

```bash
zensical serve -o   # opens at localhost:8000/dbt-duckdb
```

## Contributing

Please open an issue or pull request on [GitHub](https://github.com/duckdb/dbt-duckdb).
