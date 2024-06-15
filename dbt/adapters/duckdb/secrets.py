from dataclasses import dataclass
from dataclasses import fields
from enum import Enum
from typing import Optional

from dbt_common.dataclass_schema import dbtClassMixin


DEFAULT_SECRET_PREFIX = "_dbt_secret_"


class SecretType(Enum):
    S3 = 0
    AZURE = 1
    R2 = 2
    GCS = 3
    HUGGINGFACE = 4


class SecretProvider(Enum):
    CONFIG = 0
    CREDENTIAL_CHAIN = 1


@dataclass
class Secret(dbtClassMixin):
    type: SecretType
    persistent: bool = False
    name: Optional[str] = None
    provider: Optional[SecretProvider] = None

    @classmethod
    def cls_from_type(cls, secret_type: Optional[SecretType]):
        if SecretType.S3 == secret_type:
            return AWSSecret

        raise ValueError(f"Secret type {secret_type} is currently not supported.")

    @classmethod
    def create(
        cls,
        secret_type: str,
        persistent: Optional[bool] = None,
        provider: Optional[str] = None,
        **kwargs,
    ):
        _secret_type = None
        _provider = None

        try:
            _secret_type = SecretType[secret_type.upper()]
        except KeyError:
            pass

        if provider is not None:
            try:
                _provider = SecretProvider[provider.upper()]
            except KeyError:
                pass

        secret_cls = cls.cls_from_type(_secret_type)
        try:
            return secret_cls(persistent=persistent, provider=_provider, **kwargs)
        except TypeError as e:
            secret_params = ", ".join([_f.name for _f in fields(secret_cls)])
            if isinstance(_secret_type, SecretType):
                raise ValueError(
                    f"Could not create secret: {str(e)}. "
                    f"Supported input arguments for secret of type {_secret_type.name}: "
                    f"{secret_params}"
                )

    def get_sql_params(self):
        params = {"type": self.type.name}

        if self.provider is not None:
            params["provider"] = self.provider.name

        params.update(
            {
                field.name: getattr(self, field.name)
                for field in fields(self)
                if hasattr(self, field.name)
                and getattr(self, field.name) is not None
                and field.name not in ["type", "persistent", "name", "provider"]
            }
        )

        return params

    def to_sql(self) -> str:
        or_replace = " OR REPLACE" if self.name else ""
        persistent = " PERSISTENT" if self.persistent is True else ""
        name = f" {self.name}" if self.name else ""
        params = self.get_sql_params()
        tab = "    "
        params_sql = f",\n{tab}".join([f"{key} {value}" for key, value in params.items()])
        sql = f"""CREATE{or_replace}{persistent} SECRET{name} (\n{tab}{params_sql}\n)"""
        return sql


@dataclass
class AWSSecret(Secret):
    type: SecretType = SecretType.S3
    chain: Optional[str] = None
    key_id: Optional[str] = None
    secret: Optional[str] = None
    region: Optional[str] = None
    session_token: Optional[str] = None
    endpoint: Optional[str] = None
    url_style: Optional[str] = None
    use_ssl: Optional[bool] = None
    url_compatibility_mode: Optional[bool] = None
    account_id: Optional[str] = None
