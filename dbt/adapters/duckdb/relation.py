import os
from typing import Optional

from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation
from dbt.exceptions import RuntimeException


@dataclass(frozen=True, eq=False, repr=False)
class DuckDBRelation(BaseRelation):
    def __post_init__(self):
        # Check for length of Postgres table/view names.
        # Check self.type to exclude test relation identifiers
        if (
            self.identifier is not None
            and self.type is not None
            and len(self.identifier) > self.relation_max_name_length()
        ):
            raise RuntimeException(
                f"Relation name '{self.identifier}' "
                f"is longer than {self.relation_max_name_length()} characters"
            )

    def relation_max_name_length(self):
        return 63

    @classmethod
    def is_path(self, db: Optional[str]) -> bool:
        # TODO: make this smarter on windows, handle S3 explicitly, etc.
        return db is not None and os.path.sep in db

    def render(self):
        if DuckDBRelation.is_path(self.database):
            # TODO: file extension?
            identifier = self.identifier
            # needs to be smarter about S3 on windows, etc.
            path_str = os.path.join(self.database, self.schema, identifier)
            return f"'{path_str}'"
        else:
            return super().render()
