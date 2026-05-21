# Table Materialization Drift: dbt-duckdb vs Global Project

Goal: identify every difference between `dbt/include/duckdb/macros/materializations/table.sql`
and the dbt-core global project default, then assess whether each difference can be moved
into a dispatched sub-macro so the materialization itself shrinks toward the default.

---

## Side-by-side summary

| # | Difference | dbt-duckdb | global project |
|---|---|---|---|
| 1 | Materialization signature | `adapter="duckdb", supported_languages=['sql','python']` | `default` (SQL only) |
| 2 | Language variable | `{% set language = model['language'] %}` | none |
| 3 | Statement call | `{% call statement('main', language=language, auto_begin=not skip_auto_begin) %}` | `{% call statement('main') %}` |
| 4 | SQL-generation macro | `create_table_as(…, language, partitioned_by, sorted_by)` | `get_create_table_as_sql(False, relation, sql)` |
| 5 | Partitioning / sorting | reads config, passes to `create_table_as` | not present |
| 6 | `skip_auto_begin` | set for DuckLake + partitions/sorts | not present |
| 7 | DuckLake docs timing | `persist_docs` deferred to after `adapter.commit()` + second commit | `persist_docs` always inside transaction |
| 8 | Index drop before first rename | `drop_indexes_on_relation(existing_relation)` before rename | not present |
| 9 | `create_indexes` placement | on **target** after both renames | on **intermediate** before renames |
| 10 | Re-check of `existing_relation` | missing | `load_cached_relation(existing_relation)` re-checked before first rename |

---

## Per-difference analysis

### 1 & 2 — `supported_languages` + `language` variable

**Movable? No — structural.**

`supported_languages` must be in the materialization signature; dbt-core uses it to decide
whether to route Python models here. `language=language` on the `statement()` call changes
the execution mode (Python vs SQL). Neither can be expressed in a sub-macro.

This is the one irreducible reason dbt-duckdb needs its own `table` materialization at all.

---

### 3 — `auto_begin=not skip_auto_begin` on the statement call

**Movable? No — structural (but scope can shrink).**

`auto_begin` is a parameter of `{% call statement() %}`, not of the SQL content. It cannot
be controlled from inside `create_table_as` or any other sub-macro.

However, `skip_auto_begin` is only set for DuckLake + partitions/sorts (difference #6).
If partitioned_by/sorted_by move inside `duckdb__create_table_as` (see #5 below), this
flag and the `auto_begin` override would need to stay but could be simplified:

```jinja
{%- set skip_auto_begin = adapter.is_ducklake(target_relation)
                          and config.get('partitioned_by') or config.get('sorted_by') -%}
```

---

### 4 — `get_create_table_as_sql` vs `create_table_as`

**Movable? Yes — already dispatched, just uses a different dispatch path.**

The global project calls `get_create_table_as_sql()` → dispatches to
`adapter__get_create_table_as_sql`. dbt-duckdb calls `create_table_as()` → dispatches to
`duckdb__create_table_as`. These are functionally equivalent dispatch mechanisms.

The dbt-duckdb version also passes `language`, `partitioned_by`, and `sorted_by` as extra
arguments. Once #5 is resolved (args moved into the macro via config), both callers
could use the same `get_create_table_as_sql` dispatch and the materialization call would
align with the global project:

```jinja
{# goal state #}
{% call statement('main') -%}
  {{ get_create_table_as_sql(False, intermediate_relation, compiled_code) }}
{%- endcall %}
```

(Python language support would still need `language=language` on the statement call.)

---

### 5 — `partitioned_by` / `sorted_by` variables

**Movable? Yes — read from `config` inside `duckdb__create_table_as`.**

Currently the materialization computes them and passes them as arguments:

```jinja
{%- set partitioned_by = duckdb__get_partitioned_by(target_relation, false) -%}
{%- set sorted_by = duckdb__get_sorted_by(target_relation, false) -%}
…
{{- create_table_as(False, intermediate_relation, compiled_code, language,
      partitioned_by=partitioned_by, sorted_by=sorted_by) }}
```

`duckdb__create_table_as` could read them directly from `config` (same as
`duckdb__get_partitioned_by` already does internally), removing them from the
materialization entirely:

```jinja
{% macro duckdb__create_table_as(temporary, relation, compiled_code, language='sql', …) %}
  {%- set partitioned_by = duckdb__get_partitioned_by(relation, temporary) -%}
  {%- set sorted_by     = duckdb__get_sorted_by(relation, temporary) -%}
  …
{% endmacro %}
```

The `skip_auto_begin` flag for DuckLake+partitions would also move — but since it
controls `auto_begin` on the `statement()` call (see #3), the materialization would
need to keep the flag and read it a different way, e.g.:

```jinja
{%- set skip_auto_begin = adapter.needs_separate_ctas_transaction(target_relation) -%}
```

where `needs_separate_ctas_transaction` is a new `@available` method that encapsulates
the DuckLake+partition check.

---

### 6 — `skip_auto_begin` (DuckLake + partitions/sorts)

See #3 and #5 above. Can be simplified once partitioned_by/sorted_by move into
`duckdb__create_table_as`, but the `auto_begin` override itself stays structural.

---

### 7 — DuckLake post-commit `persist_docs`

**Movable? Yes — override `duckdb__persist_docs`.**

Currently the materialization has conditional logic:

```jinja
{% if not post_commit_ducklake_docs %}
  {% do persist_docs(target_relation, model) %}
{% endif %}
{{ adapter.commit() }}
{% if post_commit_ducklake_docs %}
  {% do persist_docs(target_relation, model) %}
  {{ adapter.commit() }}
{% endif %}
```

This can move entirely into a `duckdb__persist_docs` override that commits first when
needed, letting the materialization use the global project's simple one-liner:

```jinja
{% macro duckdb__persist_docs(relation, model, for_relation=true, for_columns=true) %}
  {% if adapter.is_ducklake(relation) %}
    {{ adapter.commit() }}
  {% endif %}
  {{ return(default__persist_docs(relation, model,
        for_relation=for_relation, for_columns=for_columns)) }}
{% endmacro %}
```

The `adapter.commit()` at the end of the materialization would then be a no-op for
DuckLake (transaction already closed), which is safe.

**Caveat**: the second `adapter.commit()` after DuckLake persist_docs is for committing
the docs-update itself. That still needs to happen. Options:
- call `adapter.commit()` inside `duckdb__persist_docs` after the docs update too, or
- accept that the final `adapter.commit()` in the materialization handles it.

---

### 8 — `drop_indexes_on_relation` before first rename

**Movable? Yes — move into `duckdb__rename_relation`.**

The global project does not drop indexes before renaming. dbt-duckdb added this to avoid
dependency errors when renaming a table that has indexes in some DuckDB versions.

Moving it into `duckdb__rename_relation` removes it from the materialization and
automatically protects every materialization (table, incremental, external):

```jinja
{% macro duckdb__rename_relation(from_relation, to_relation) -%}
  {% set target_name = adapter.quote_as_configured(to_relation.identifier, 'identifier') %}
  {%- if adapter.is_iceberg(from_relation) and adapter.has_open_transaction() -%}
    {{ adapter.commit() }}
  {%- else -%}
    {% do drop_indexes_on_relation(from_relation) %}
  {%- endif -%}
  {% call statement('rename_relation') -%}
    alter {{ to_relation.type }} {{ from_relation }} rename to {{ target_name }}
  {%- endcall %}
{% endmacro %}
```

(The `else` branch avoids redundantly dropping indexes on Iceberg relations, which
don't have DuckDB-style indexes anyway.)

---

### 9 — `create_indexes` placement

**Movable? Yes — consequence of moving #8 into `rename_relation`.**

Global project creates indexes on the *intermediate* relation before renames, then the
rename carries the indexes along to the target name. dbt-duckdb drops indexes on the
existing relation, renames, then creates indexes on the final target — because the drop
was needed to make the first rename safe.

Once the index drop moves into `duckdb__rename_relation` (#8), the materialization could
align with the global project and call `create_indexes(intermediate_relation)` before the
renames. Indexes would then travel with the table through both renames.

This removes the `{% do create_indexes(target_relation) %}` call that currently sits after
the renames in dbt-duckdb.

---

### 10 — Missing re-check of `existing_relation` before first rename

**Action: align with global project (this is a gap, not a feature).**

The global project defensively re-checks `existing_relation` immediately before the first
rename in case it was dropped between setup and execution:

```jinja
{% set existing_relation = load_cached_relation(existing_relation) %}
{% if existing_relation is not none %}
    {{ adapter.rename_relation(existing_relation, backup_relation) }}
{% endif %}
```

dbt-duckdb skips this re-check. It should be added.

---

## Proposed goal state for `table.sql`

After all movable differences are extracted, the dbt-duckdb materialization shrinks to:

```jinja
{% materialization table, adapter="duckdb", supported_languages=['sql', 'python'] %}

  {%- set language = model['language'] -%}
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') %}
  {%- set intermediate_relation = make_intermediate_relation(target_relation) -%}
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
  {% set grant_config = config.get('grants') %}

  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {%- set skip_auto_begin = adapter.needs_separate_ctas_transaction(target_relation) -%}

  -- build model
  {% call statement('main', language=language, auto_begin=not skip_auto_begin) -%}
    {{ get_create_table_as_sql(False, intermediate_relation, compiled_code) }}
  {%- endcall %}

  {% do create_indexes(intermediate_relation) %}

  -- cleanup
  {% if existing_relation is not none %}
    {% set existing_relation = load_cached_relation(existing_relation) %}
    {% if existing_relation is not none %}
      {{ adapter.rename_relation(existing_relation, backup_relation) }}
    {% endif %}
  {% endif %}

  {{ adapter.rename_relation(intermediate_relation, target_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
```

The only remaining differences from the global project default are:
- `adapter="duckdb", supported_languages=['sql', 'python']` (line 1) — irreducible
- `language=language` on the statement call — irreducible (Python support)
- `auto_begin=not skip_auto_begin` + `skip_auto_begin` variable — irreducible as long as
  DuckLake partition/sort CTAS requires a separate transaction
- `get_create_table_as_sql` is called with `compiled_code` not `sql` — these are the
  same value; aligning variable names is trivial

---

## Work breakdown for a follow-on PR

| Task | Macro to change | Effort |
|---|---|---|
| Move index drop before rename | `duckdb__rename_relation` | small |
| Move Iceberg commit before rename (already done) | `duckdb__rename_relation` | done |
| Move DuckLake post-commit docs | new `duckdb__persist_docs` | small |
| Move partitioned_by/sorted_by into create_table_as | `duckdb__create_table_as` | medium |
| Add `needs_separate_ctas_transaction()` adapter method | `impl.py` | small |
| Add defensive re-check of `existing_relation` | `table.sql` | trivial |
| Align `create_indexes` placement (intermediate not target) | `table.sql` | small |
