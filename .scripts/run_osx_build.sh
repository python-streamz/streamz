#!/usr/bin/env bash

set -x

echo -e "\n\nInstalling a fresh version of Miniforge."
if [[ ${CI} == "travis" ]]; then
  echo -en 'travis_fold:start:install_miniforge\\r'
fi
MINIFORGE_URL="https://github.com/conda-forge/miniforge/releases/latest/download"
MINIFORGE_FILE="Miniforge3-MacOSX-x86_64.sh"
curl -L -O "${MINIFORGE_URL}/${MINIFORGE_FILE}"
bash $MINIFORGE_FILE -b
if [[ ${CI} == "travis" ]]; then
  echo -en 'travis_fold:end:install_miniforge\\r'
fi

echo -e "\n\nConfiguring conda."
if [[ ${CI} == "travis" ]]; then
  echo -en 'travis_fold:start:configure_conda\\r'
fi

source ${HOME}/miniforge3/etc/profile.d/conda.sh
conda activate base

echo -e "\n\nInstalling conda-forge-ci-setup=3 and conda-build."
conda install -n base --quiet --yes "conda-forge-ci-setup=3" conda-build pip



echo -e "\n\nSetting up the condarc and mangling the compiler."
setup_conda_rc ./ ./recipe ./.ci_support/${CONFIG}.yaml
mangle_compiler ./ ./recipe .ci_support/${CONFIG}.yaml

echo -e "\n\nMangling homebrew in the CI to avoid conflicts."
/usr/bin/sudo mangle_homebrew
/usr/bin/sudo -k

echo -e "\n\nRunning the build setup script."
source run_conda_forge_build_setup


if [[ ${CI} == "travis" ]]; then
  echo -en 'travis_fold:end:configure_conda\\r'
fi

set -e

echo -e "\n\nMaking the build clobber file and running the build."
make_build_number ./ ./recipe ./.ci_support/${CONFIG}.yaml

conda build ./recipe -m ./.ci_support/${CONFIG}.yaml --suppress-variables --clobber-file ./.ci_support/clobber_${CONFIG}.yaml ${EXTRA_CB_OPTIONS:-}

if [[ "${UPLOAD_PACKAGES}" != "False" ]]; then
  echo -e "\n\nUploading the packages."
  upload_package  ./ ./recipe ./.ci_support/${CONFIG}.yaml
fi