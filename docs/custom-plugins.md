# Writing Custom Plugins

The dbt-duckdb [plugin system](dbt/adapters/duckdb/plugins/__init__.py) lets you extend dbt-duckdb with custom functionality — UDFs, source loaders, and external file writers.

## Creating a plugin

Create a Python module that defines a class named `Plugin` inheriting from `dbt.adapters.duckdb.plugins.BasePlugin`:

```python
from dbt.adapters.duckdb.plugins import BasePlugin
from dbt.adapters.duckdb.utils import SourceConfig, TargetConfig

class Plugin(BasePlugin):
    def initialize(self, config: dict) -> None:
        # Called once when the Plugin instance is created.
        # `config` is the dict from the `config:` key in your profile.
        pass

    def configure_connection(self, conn) -> None:
        # Called with the DuckDBPyConnection after it is created.
        # Use this to register custom UDFs or set connection-level config.
        pass

    def load(self, source_config: SourceConfig):
        # Called when a dbt source backed by this plugin is referenced.
        # Return a DataFrame-like object (Pandas, Polars, Arrow, DuckDB Relation)
        # or None to skip.
        pass

    def store(self, target_config: TargetConfig) -> None:
        # Called after an `external` materialization writes its file.
        # Use this to register the file with an external catalog (e.g., Glue).
        pass
```

You only need to implement the methods relevant to your use case — all methods have no-op defaults in `BasePlugin`.

## Plugin methods

### `initialize(config)`

Called once when the plugin instance is created. The `config` argument is the dict from the `config:` key in your profile. Use this for any one-time setup — connecting to an external service, loading credentials, etc.

### `configure_connection(conn)`

Called with the `DuckDBPyConnection` object after it is opened. Use this to register Python UDFs that can then be used in SQL models:

```python
import duckdb

def configure_connection(self, conn: duckdb.DuckDBPyConnection) -> None:
    conn.create_function("my_udf", lambda x: x * 2, [duckdb.typing.INTEGER], duckdb.typing.INTEGER)
```

### `load(source_config)`

Called when a dbt source backed by this plugin is referenced. The `source_config` is a [`SourceConfig`](dbt/adapters/duckdb/utils.py) instance containing the source's configuration. Return a DataFrame-like object that DuckDB can turn into a table, or `None` to skip loading.

### `store(target_config)`

Called after an `external` materialization writes its output file. The `target_config` is a [`TargetConfig`](dbt/adapters/duckdb/utils.py) instance. Use this to perform post-write operations — for example, the built-in `glue` plugin uses `store` to register the output file with AWS Glue, and the `sqlalchemy` plugin uses it to upload the DataFrame to an external database.

## Registering your plugin

Reference your plugin by its full module path in the profile:

```yaml
default:
  outputs:
    dev:
      type: duckdb
      path: /tmp/dbt.duckdb
      plugins:
        - module: my_project.plugins.my_custom_plugin
          alias: custom
          config:
            my_setting: my_value
  target: dev
```

If your module is not on the default Python path, add its directory via `module_paths` in your profile. See [Configuration](configuration/index.md#local-python-modules).

## Examples

The built-in plugins are good references for each use case:

- UDF registration — see [sqlalchemy plugin](dbt/adapters/duckdb/plugins/sqlalchemy.py)
- Source loading — see [gsheet plugin](dbt/adapters/duckdb/plugins/gsheet.py) or [excel plugin](dbt/adapters/duckdb/plugins/excel.py)
- External catalog registration — see [glue plugin](dbt/adapters/duckdb/plugins/glue.py)
