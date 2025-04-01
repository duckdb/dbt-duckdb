from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from dbt_common.dataclass_schema import dbtClassMixin


DEFAULT_SECRET_PREFIX = "_dbt_secret_"


@dataclass
class Secret(dbtClassMixin):
    type: str
    persistent: Optional[bool] = False
    name: Optional[str] = None
    provider: Optional[str] = None
    scope: Optional[Union[str, List[str]]] = None
    secret_kwargs: Optional[Dict[str, Any]] = None

    @classmethod
    def create(
        cls,
        secret_type: str,
        persistent: Optional[bool] = None,
        name: Optional[str] = None,
        provider: Optional[str] = None,
        scope: Optional[Union[str, List[str]]] = None,
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
        name = f" {self.name}" if self.name else ""
        or_replace = " OR REPLACE" if name else ""
        persistent = " PERSISTENT" if self.persistent is True else ""
        tab = "    "
        params = self.to_dict(omit_none=True)
        params.update(params.pop("secret_kwargs", {}))

        scope_value: Optional[List[str]] = None
        raw_scope = params.get("scope")
        if isinstance(raw_scope, str):
            scope_value = [raw_scope]
        elif isinstance(raw_scope, list):
            scope_value = raw_scope

        if scope_value is not None:
            params.pop("scope", None)
            params_sql: List[str] = []
            for key, value in params.items():
                if value is not None and key not in ["name", "persistent"]:
                    if key not in ["type", "provider", "extra_http_headers"]:
                        params_sql.append(f"{key} '{value}'")
                    else:
                        params_sql.append(f"{key} {value}")
            for s in scope_value:
                params_sql.append(f"scope '{s}'")

            params_sql_str = f",\n{tab}".join(params_sql)
        else:
            params_sql_list = [
                f"{key} '{value}'"
                if key not in ["type", "provider", "extra_http_headers"]
                else f"{key} {value}"
                for key, value in params.items()
                if value is not None and key not in ["name", "persistent"]
            ]
            params_sql_str = f",\n{tab}".join(params_sql_list)

        sql = f"""CREATE{or_replace}{persistent} SECRET{name} (\n{tab}{params_sql_str}\n)"""
        return sql
