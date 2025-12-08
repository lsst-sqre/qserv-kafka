#!/bin/bash

# This script installs additional packages used by the dependency image but
# not needed by the runtime image, such as additional packages required to
# build Python dependencies.
#
# Since the base image wipes all the apt caches to clean up the image that
# will be reused by the runtime image, we unfortunately have to do another
# apt-get update here, which wastes some time and network.

# Bash "strict mode", to help catch problems and bugs in the shell
# script. Every bash script you write should include this. See
# http://redsymbol.net/articles/unofficial-bash-strict-mode/ for
# details.
set -euo pipefail

# Display each command as it's run.
set -x

# Tell apt-get we're never going to be able to give manual feedback.
export DEBIAN_FRONTEND=noninteractive

# Refresh lists. This is normally not necessary since it's done by
# install-base-packages.sh, but that step might be skipped due to caching.
apt-get update

# build-essential: used by Python dependencies that build C modules
# git: required during package installation for setuptools-scm
# libffi-dev: sometimes needed to build cffi (a cryptography dependency)
# zlib1g-dev: sometimes needed to build aiokafka
apt-get -y install --no-install-recommends \
    build-essential git libffi-dev zlib1g-dev
