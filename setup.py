#!/usr/bin/env python
from setuptools import find_packages
from distutils.core import setup
import os

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md')) as f:
    long_description = f.read()

package_name = "dbt-duckdb"
package_version = "0.18.1"
description = """The duckdb adpter plugin for dbt (data build tool)"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=long_description,
    long_description_content_type='text/markdown',

    author='Josh Wills',
    author_email='joshwills+dbt@gmail.com',
    url='https://github.com/jwills/dbt-duckdb',
    packages=find_packages(),
    package_data={
        'dbt': [
            'include/duckdb/dbt_project.yml',
            'include/duckdb/macros/*.sql',
            'include/duckdb/macros/**/*.sql',
        ]
    },
    install_requires=[
        'dbt-core=={}'.format(package_version),
        'duckdb>=0.2.2',
    ]
)
