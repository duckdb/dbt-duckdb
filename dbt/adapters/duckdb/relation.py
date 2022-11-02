from typing import Any, Optional, Type

from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation, Self
from dbt.exceptions import NotImplementedException
from dbt.contracts.graph.parsed import ParsedSourceDefinition
from dbt.contracts.graph.unparsed import ExternalTable


def _create_external_location(external: ExternalTable) -> str:
    location = external.location
    if external.file_format:
        if external.file_format == "parquet":
            func = "read_parquet"
        elif external.file_format == "csv":
            func = "read_csv_auto"
        else:
            raise NotImplementedException(
                f"Unsupported file format: {external.file_format}"
            )
        options = external.extra.get("options", None)
        if options:
            option_str = ", ".join([f"{k} = {v}" for k, v in options.items()])
            return f"{func}('{location}', {option_str})"
        else:
            return f"{func}('{location}')"
    else:
        return f"'{location}'"


@dataclass(frozen=True, eq=False, repr=False)
class DuckDBRelation(BaseRelation):
    external_location: Optional[str] = None

    @classmethod
    def create_from_source(
        cls: Type[Self], source: ParsedSourceDefinition, **kwargs: Any
    ) -> Self:
        if source.external:
            kwargs["external_location"] = _create_external_location(source.external)
        return super().create_from_source(source, **kwargs)  # type: ignore

    def render(self) -> str:
        if self.external_location:
            return self.external_location
        else:
            return super().render()
