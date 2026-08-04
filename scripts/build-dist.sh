#!/bin/bash

set -eo pipefail

DBT_PATH="$( cd "$(dirname "$0")/.." ; pwd -P )"

set -x

rm -rf "$DBT_PATH"/dist

cd "$DBT_PATH"
uv build

set +x
