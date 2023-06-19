import pandas as pd

from ..utils import TargetConfig


def target_to_df(target_config: TargetConfig) -> pd.DataFrame:
    """Load a dataframe from a target config."""
    location = target_config.location
    if location is None:
        raise Exception("Target config does not have a location")
    if location.format == "csv":
        return pd.read_csv(location.path)
    elif location.format == "parquet":
        return pd.read_parquet(location.path)
    else:
        raise Exception(f"Unsupported format: {location.format}")
