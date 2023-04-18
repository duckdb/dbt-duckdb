#!/usr/bin/env python
import os
import re

from setuptools import find_namespace_packages
from setuptools import setup

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.md")) as f:
    long_description = f.read()

package_name = "dbt-duckdb"


def _dbt_duckdb_version():
    _version_path = os.path.join(this_directory, "dbt", "adapters", "duckdb", "__version__.py")
    _version_pattern = r"""version\s*=\s*["'](.+)["']"""
    with open(_version_path) as f:
        match = re.search(_version_pattern, f.read().strip())
        if match is None:
            raise ValueError(f"invalid version at {_version_path}")
        return match.group(1)


package_version = _dbt_duckdb_version()
description = """The duckdb adapter plugin for dbt (data build tool)"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Josh Wills",
    author_email="joshwills+dbt@gmail.com",
    url="https://github.com/jwills/dbt-duckdb",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        "dbt-core~=1.5.0rc1",
        "duckdb>=0.5.0",
    ],
    extras_require={
        "glue": ["boto3", "mypy-boto3-glue"],
    },
)
