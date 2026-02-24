#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "${DIR}/.."
export VERSION=$(python "${DIR}/../setup.py" --version)
docker run --rm -p 8888:8888 "streamz:${VERSION}"
