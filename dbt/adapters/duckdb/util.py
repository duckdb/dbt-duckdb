import os
from typing import Optional


def is_filepath(field: Optional[str]) -> bool:
    return field is not None and os.path.sep in field
