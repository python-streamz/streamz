# Contributing to Streamz

## Streamz Conda Environment
For CI, streamz uses environment files to create conda environments.
Contributors can reuse those files to create development environments. A few
basic quick start commands for creating a using a streamz conda environment
are found below.

Environment files exist for all supported version under:
```shell
./ci/environment-py${VERSION}.yml
```

### Creating Conda Development Environment
Creating a streamz conda environment can be achieved by running
```shell
export VERSION="314"
conda env create -f "./ci/environment-py$VERSION.yml" -n "streamz$VERSION"
conda activate "streamz${VERSION}"
pip install flake8
pip install --editable=. --no-deps
```

### Using Conda Environment
The streamz conda environment can be activated in the future by running
```shell
conda activate "streamz${VERSION}"
```

### Creating a Non-Conda Environment
If you do not want to use conda, there is a ```requirements-test.txt``` file
in the proejct root that will install packages into the current python
environment:
```shell
pip install --editable=. --requirement=requirements-test.txt
pip install flake8
```
We do not use it for CI, so please submit a PR to update it if you notice
that it has drifted away from the conda environments. (I.e. you cannot run
the tests.)

### Run Software Tests
Once you activate your environment of choice, running pytest and flake8 will work:
```shell
pytest -vvv
flake8 streamz
```