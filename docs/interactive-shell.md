# Interactive Shell

As of version 1.9.3, dbt-duckdb includes an interactive shell that combines dbt commands with the [DuckDB UI](https://duckdb.org/2025/03/12/duckdb-ui.html) for exploratory development.

## Starting the shell

```bash
python -m dbt.adapters.duckdb.cli
```

To use a specific profile:

```bash
python -m dbt.adapters.duckdb.cli --profile my_profile
```

## What happens on launch

When you start the shell, it automatically:

1. Runs `dbt debug` to verify your connection
2. Parses your dbt project
3. Launches the DuckDB UI in your browser for visual data exploration

## Available commands

All standard dbt commands are available inside the shell:

| Command | Description |
| --- | --- |
| `run` | Run dbt models |
| `test` | Run tests on dbt models |
| `build` | Build and test dbt models |
| `seed` | Load seed files |
| `snapshot` | Run snapshots |
| `compile` | Compile models without running them |
| `parse` | Parse the project |
| `debug` | Debug the connection |
| `deps` | Install dependencies |
| `list` | List project resources |

## Autocompletion

Install `iterfzf` to enable model name autocompletion:

```bash
pip install iterfzf
```

## Typical workflow

1. Start the shell — the DuckDB UI opens automatically
2. Browse your project's existing models and data in the UI
3. Run `build` to build and test your models
4. Inspect the updated results in the UI
5. Iterate
