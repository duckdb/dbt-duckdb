import pytest

from dbt.exceptions import ParsingError
from dbt.tests.util import run_dbt, relation_from_name

try:
    from dbt.tests.adapter.incremental.test_incremental_microbatch import (
        BaseMicrobatch,
    )
    MICROBATCH_AVAILABLE = True
except ImportError:
    BaseMicrobatch = None
    MICROBATCH_AVAILABLE = False


# Validation models
models__microbatch_missing_event_time = """
{{ config(materialized='incremental', incremental_strategy='microbatch') }}

select 1 as id, '2025-01-01 00:00:00'::timestamp as event_time
"""

models__microbatch_missing_batch_context = """
{{ config(materialized='incremental', incremental_strategy='microbatch', event_time='event_time') }}

select 1 as id, '2025-01-01 00:00:00'::timestamp as event_time
"""

models__microbatch_unique_key_not_supported = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    unique_key='id',
    event_time='event_time',
    begin='2025-01-01',
    batch_size='day'
) }}

select 1 as id, '2025-01-01 00:00:00'::timestamp as event_time
"""

# Scenario models
models__microbatch_exec_input = """
{{ config(materialized='table') }}

select 1 as id, '2025-01-01 00:00:00'::timestamp as event_time
union all
select 2 as id, '2025-01-02 00:00:00'::timestamp as event_time
union all
select 3 as id, '2025-01-03 00:00:00'::timestamp as event_time
"""

models__microbatch_exec = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_time',
    batch_size='day',
    begin=modules.datetime.datetime(2025, 1, 1, 0, 0, 0)
) }}

select id, event_time from {{ ref('microbatch_exec_input') }}
"""

models__microbatch_event_date_input = """
{{ config(materialized='table') }}

select 1 as id, '2025-01-01'::date as event_date
union all
select 2 as id, '2025-01-02'::date as event_date
union all
select 3 as id, '2025-01-03'::date as event_date
"""

models__microbatch_event_date = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_date',
    batch_size='day',
    begin=modules.datetime.datetime(2025, 1, 1, 0, 0, 0)
) }}

select id, event_date from {{ ref('microbatch_event_date_input') }}
"""

models__microbatch_batch_hour_input = """
{{ config(materialized='table') }}

select 1 as id, '2025-01-01 00:00:00'::timestamp as event_time
union all
select 2 as id, '2025-01-01 01:00:00'::timestamp as event_time
union all
select 3 as id, '2025-01-01 02:00:00'::timestamp as event_time
"""

models__microbatch_batch_hour = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_time',
    batch_size='hour',
    begin=modules.datetime.datetime(2025, 1, 1, 0, 0, 0)
) }}

select id, event_time from {{ ref('microbatch_batch_hour_input') }}
"""

models__microbatch_batch_month_input = """
{{ config(materialized='table') }}

select 1 as id, '2025-01-01'::timestamp as event_time
union all
select 2 as id, '2025-02-01'::timestamp as event_time
union all
select 3 as id, '2025-03-01'::timestamp as event_time
"""

models__microbatch_batch_month = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_time',
    batch_size='month',
    begin=modules.datetime.datetime(2025, 1, 1, 0, 0, 0)
) }}

select id, event_time from {{ ref('microbatch_batch_month_input') }}
"""

models__microbatch_reprocess_input = """
{{ config(materialized='table') }}

select 1 as id, '2025-01-01 00:00:00'::timestamp as event_time, 'alpha' as note
union all
select 2 as id, '2025-01-02 00:00:00'::timestamp as event_time, 'bravo' as note
union all
select 3 as id, '2025-01-03 00:00:00'::timestamp as event_time, 'charlie' as note
"""

models__microbatch_reprocess = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_time',
    batch_size='day',
    begin=modules.datetime.datetime(2025, 1, 1, 0, 0, 0)
) }}

select id, event_time, note from {{ ref('microbatch_reprocess_input') }}
"""


@pytest.mark.skipif(not MICROBATCH_AVAILABLE, reason="Microbatch tests require dbt-core >= 1.9")
class TestMicrobatch(BaseMicrobatch):
    """DuckDB overrides for dbt-core microbatch tests."""

    @pytest.fixture(scope="class")
    def microbatch_model_sql(self) -> str:
        return """
        {{ config(materialized='incremental', incremental_strategy='microbatch', event_time='event_time', batch_size='day', begin=modules.datetime.datetime(2025, 1, 1, 0, 0, 0)) }}
        select * from {{ ref('input_model') }}
        """

    @pytest.fixture(scope="class")
    def input_model_sql(self) -> str:
        return """
            {{ config(materialized='table', event_time='event_time') }}
            select 1 as id, '2025-01-01 00:00:00'::timestamp as event_time
            union all
            select 2 as id, '2025-01-02 00:00:00'::timestamp as event_time
            union all
            select 3 as id, '2025-01-03 00:00:00'::timestamp as event_time
        """

    def test_run_with_event_time(self, project):
        run_dbt(
            [
                "run",
                "--select",
                "input_model microbatch_model",
                "--event-time-start",
                "2025-01-01",
                "--event-time-end",
                "2025-01-04",
            ],
            expect_pass=True,
        )

        result = project.run_sql(
            f"select count(*) from {relation_from_name(project.adapter, 'microbatch_model')}",
            fetch="one",
        )
        assert result[0] == 3


@pytest.mark.skipif(not MICROBATCH_AVAILABLE, reason="Microbatch tests require dbt-core >= 1.9")
class TestMicrobatchValidationMissingEventTime:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "microbatch_missing_event_time.sql": models__microbatch_missing_event_time,
        }

    def test_missing_event_time_config(self, project):
        with pytest.raises(ParsingError) as excinfo:
            run_dbt(
                ["run", "--select", "microbatch_missing_event_time"], expect_pass=False
            )

        assert "must provide an 'event_time'" in str(excinfo.value)


@pytest.mark.skipif(not MICROBATCH_AVAILABLE, reason="Microbatch tests require dbt-core >= 1.9")
class TestMicrobatchValidationMissingBatchContext:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "microbatch_missing_batch_context.sql": models__microbatch_missing_batch_context,
        }

    def test_missing_batch_context(self, project):
        with pytest.raises(ParsingError) as excinfo:
            run_dbt(
                ["run", "--select", "microbatch_missing_batch_context"], expect_pass=False
            )

        assert "must provide a 'begin'" in str(excinfo.value)


@pytest.mark.skipif(not MICROBATCH_AVAILABLE, reason="Microbatch tests require dbt-core >= 1.9")
class TestMicrobatchUniqueKeyNotSupported:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "microbatch_unique_key_not_supported.sql": models__microbatch_unique_key_not_supported,
        }

    def test_unique_key_not_supported(self, project, capsys):
        run_dbt(
            ["run", "--select", "microbatch_unique_key_not_supported"],
            expect_pass=False,
        )
        captured = capsys.readouterr()
        assert "does not support 'unique_key'" in (captured.out + captured.err)


@pytest.mark.skipif(not MICROBATCH_AVAILABLE, reason="Microbatch tests require dbt-core >= 1.9")
class TestMicrobatchScenarios:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "microbatch_exec_input.sql": models__microbatch_exec_input,
            "microbatch_exec.sql": models__microbatch_exec,
            "microbatch_event_date_input.sql": models__microbatch_event_date_input,
            "microbatch_event_date.sql": models__microbatch_event_date,
            "microbatch_batch_hour_input.sql": models__microbatch_batch_hour_input,
            "microbatch_batch_hour.sql": models__microbatch_batch_hour,
            "microbatch_batch_month_input.sql": models__microbatch_batch_month_input,
            "microbatch_batch_month.sql": models__microbatch_batch_month,
            "microbatch_reprocess_input.sql": models__microbatch_reprocess_input,
            "microbatch_reprocess.sql": models__microbatch_reprocess,
        }

    def _run_with_bounds(
        self,
        selects,
        project,
        expect_pass=True,
        full_refresh=False,
        start="2025-01-01",
        end="2025-01-04",
    ):
        args = ["run", "--select", selects, "--event-time-start", start, "--event-time-end", end]
        if full_refresh:
            args.append("--full-refresh")
        run_dbt(args, expect_pass=expect_pass)

    def test_microbatch_runs_twice_without_changes(self, project):
        self._run_with_bounds(
            "microbatch_exec_input microbatch_exec", project, expect_pass=True, full_refresh=True
        )

        self._run_with_bounds("microbatch_exec", project, expect_pass=True)

        relation = relation_from_name(project.adapter, "microbatch_exec")
        count = project.run_sql(
            f"SELECT COUNT(*) as count FROM {relation}", fetch="one"
        )
        assert count[0] == 3

    def test_microbatch_inserts_new_batches(self, project):
        self._run_with_bounds(
            "microbatch_exec_input microbatch_exec", project, expect_pass=True, full_refresh=True
        )

        relation = relation_from_name(project.adapter, "microbatch_exec")
        count = project.run_sql(
            f"SELECT COUNT(*) as count FROM {relation}", fetch="one"
        )
        assert count[0] == 3

        source = relation_from_name(project.adapter, "microbatch_exec_input")
        project.run_sql(
            f"insert into {source} (id, event_time) values "
            "(4, '2025-01-02 12:00:00'::timestamp), "
            "(5, '2025-01-03 12:00:00'::timestamp)"
        )

        self._run_with_bounds("microbatch_exec", project, expect_pass=True)
        count = project.run_sql(
            f"SELECT COUNT(*) as count FROM {relation}", fetch="one"
        )
        assert count[0] == 5

    def test_microbatch_supports_date_event_time(self, project):
        self._run_with_bounds(
            "microbatch_event_date_input microbatch_event_date",
            project,
            expect_pass=True,
            full_refresh=True,
        )

        relation = relation_from_name(project.adapter, "microbatch_event_date")
        count = project.run_sql(
            f"SELECT COUNT(*) as count FROM {relation}", fetch="one"
        )
        assert count[0] == 3

        source = relation_from_name(project.adapter, "microbatch_event_date_input")
        project.run_sql(
            f"insert into {source} (id, event_date) values "
            "(4, '2025-01-02'::date), (5, '2025-01-03'::date)"
        )

        self._run_with_bounds("microbatch_event_date", project, expect_pass=True)
        count = project.run_sql(
            f"SELECT COUNT(*) as count FROM {relation}", fetch="one"
        )
        assert count[0] == 5

    def test_microbatch_supports_hour_batch_size(self, project):
        self._run_with_bounds(
            "microbatch_batch_hour_input microbatch_batch_hour",
            project,
            expect_pass=True,
            full_refresh=True,
        )

        relation = relation_from_name(project.adapter, "microbatch_batch_hour")
        count = project.run_sql(
            f"SELECT COUNT(*) as count FROM {relation}", fetch="one"
        )
        assert count[0] == 3

        source = relation_from_name(project.adapter, "microbatch_batch_hour_input")
        project.run_sql(
            f"insert into {source} (id, event_time) values "
            "(4, '2025-01-01 00:30:00'::timestamp), "
            "(5, '2025-01-01 00:45:00'::timestamp)"
        )

        self._run_with_bounds("microbatch_batch_hour", project, expect_pass=True)
        count = project.run_sql(
            f"SELECT COUNT(*) as count FROM {relation}", fetch="one"
        )
        assert count[0] == 5

    def test_microbatch_supports_month_batch_size(self, project):
        self._run_with_bounds(
            "microbatch_batch_month_input microbatch_batch_month",
            project,
            expect_pass=True,
            full_refresh=True,
            end="2025-04-01",
        )

        relation = relation_from_name(project.adapter, "microbatch_batch_month")
        count = project.run_sql(
            f"SELECT COUNT(*) as count FROM {relation}", fetch="one"
        )
        assert count[0] == 3

        source = relation_from_name(project.adapter, "microbatch_batch_month_input")
        project.run_sql(
            f"insert into {source} (id, event_time) values "
            "(4, '2025-02-01'::timestamp), (5, '2025-03-01'::timestamp)"
        )

        self._run_with_bounds(
            "microbatch_batch_month", project, expect_pass=True, end="2025-04-01"
        )
        count = project.run_sql(
            f"SELECT COUNT(*) as count FROM {relation}", fetch="one"
        )
        assert count[0] == 5

    def test_microbatch_reprocesses_existing_batch(self, project):
        self._run_with_bounds(
            "microbatch_reprocess_input microbatch_reprocess",
            project,
            expect_pass=True,
            full_refresh=True,
        )

        relation = relation_from_name(project.adapter, "microbatch_reprocess")
        note = project.run_sql(
            f"SELECT note FROM {relation} WHERE id = 2", fetch="one"
        )
        assert note[0] == "bravo"

        source = relation_from_name(project.adapter, "microbatch_reprocess_input")
        project.run_sql(
            f"update {source} set note = 'bravo-updated' where id = 2"
        )
        project.run_sql(
            f"insert into {source} (id, event_time, note) values "
            "(4, '2025-01-02 12:00:00'::timestamp, 'delta')"
        )

        self._run_with_bounds("microbatch_reprocess", project, expect_pass=True)

        count = project.run_sql(
            f"SELECT COUNT(*) as count FROM {relation}", fetch="one"
        )
        assert count[0] == 4

        note = project.run_sql(
            f"SELECT note FROM {relation} WHERE id = 2", fetch="one"
        )
        assert note[0] == "bravo-updated"
