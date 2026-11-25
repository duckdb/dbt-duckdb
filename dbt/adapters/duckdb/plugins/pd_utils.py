from typing import Any
from typing import Dict

import pandas as pd

from ..utils import TargetConfig


def _unquote(value: Any) -> Any:
    if isinstance(value, str) and len(value) >= 2 and value[0] == value[-1] == "'":
        return value[1:-1]
    return value


def _csv_read_kwargs(target_config: TargetConfig) -> Dict[str, Any]:
    """The pandas.read_csv arguments that match the model's CSV write options."""
    config = target_config.config
    options = {str(k).lower(): _unquote(v) for k, v in (config.get("options") or {}).items()}
    if config.get("delimiter"):
        # legacy top-level write option, which takes precedence over options
        options["delimiter"] = _unquote(config.get("delimiter"))

    kwargs: Dict[str, Any] = {}
    for key in ("delimiter", "delim", "sep"):
        if options.get(key):
            kwargs["sep"] = options[key]
            break
    if str(options.get("header", True)).lower() in ("false", "0"):
        kwargs["header"] = None
        kwargs["names"] = [c.column for c in target_config.column_list]
    compression = str(options.get("compression", "auto")).lower()
    if compression in ("gzip", "zstd"):
        kwargs["compression"] = compression
    elif compression in ("none", "uncompressed"):
        kwargs["compression"] = None
    return kwargs


def target_to_df(target_config: TargetConfig) -> pd.DataFrame:
    """Load a dataframe from a target config."""
    location = target_config.location
    if location is None:
        raise Exception("Target config does not have a location")
    if location.format == "csv":
        return pd.read_csv(location.path, **_csv_read_kwargs(target_config))
    elif location.format == "parquet":
        return pd.read_parquet(location.path)
    else:
        raise Exception(f"Unsupported format: {location.format}")
