#!/bin/bash

####################################################################################

REMOTE=$1
DEST_PATH="/data/shar"

####################################################################################

# TODO: do not copy over unnecessary files i.e. ./.git/*, only scripts.
scp -r "${PWD}" ${REMOTE}:${DEST_PATH}