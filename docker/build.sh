#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "${DIR}/.."
export VERSION=$(python "${DIR}/../setup.py" --version)
docker build -t "streamz:${VERSION}" .
