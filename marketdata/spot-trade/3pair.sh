#!/bin/bash

####################################################################################

# Remove Extraneous Files

# aws s3 ls s3://amberdata-repository/nkl/trades/ | sed -e 's# *PRE \([^/]*\)/#\1#' | sort | while read -r PAIR
# do
#     [[ "${PAIR}" < "2014" ]] && aws s3 rm --recursive "s3://amberdata-repository/nkl/trades/${PAIR}/"
# done

# Remove old dates

PAIR=$1

aws s3 ls "s3://amberdata-shar/nkl/trades/${PAIR}/" | sed -e 's# *PRE \([^/]*\)/#\1#' | sort | while read -r DATE
do
    [[ "${DATE}" < "2021" ]] && aws s3 rm --recursive "s3://amberdata-shar/nkl/trades/${PAIR}/${DATE}"
done