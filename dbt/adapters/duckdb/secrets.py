from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import Optional

from dbt_common.dataclass_schema import dbtClassMixin


DEFAULT_SECRET_PREFIX = "_dbt_secret_"


@dataclass
class Secret(dbtClassMixin):
    type: str
    persistent: Optional[bool] = False
    name: Optional[str] = None
    provider: Optional[str] = None
    scope: Optional[str] = None
    secret_kwargs: Optional[Dict[str, Any]] = None

    @classmethod
    def create(
        cls,
        secret_type: str,
        persistent: Optional[bool] = None,
        name: Optional[str] = None,
        provider: Optional[str] = None,
        scope: Optional[str] = None,
        **kwargs,
    ):
        # Create and return Secret
        return cls(
            type=secret_type,
            persistent=persistent,
            name=name,
            provider=provider,
            scope=scope,
            secret_kwargs=kwargs,
        )

    def to_sql(self) -> str:
        params = self.to_dict()
        params.update(params.pop("secret_kwargs"))
        name = params.pop("name")
        name = f" {name}" if name else ""
        or_replace = " OR REPLACE" if name else ""
        persistent = " PERSISTENT" if params.pop("persistent") is True else ""
        tab = "    "
        params_sql = f",\n{tab}".join(
            [f"{key} {value}" for key, value in params.items() if value is not None]
        )
        sql = f"""CREATE{or_replace}{persistent} SECRET{name} (\n{tab}{params_sql}\n)"""
        return sql
