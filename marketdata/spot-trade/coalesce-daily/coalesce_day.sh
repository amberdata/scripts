#!/bin/bash

# Script takes ~6 min (i.e. 2021-10-20 all pairs)
####################################################################################

START_PAIR=$1
END_PAIR=$2
DATE=$3
IGNORE_SUCCESS=$4

####################################################################################

PAIRS=( $(
aws s3 ls s3://amberdata-marketdata/trade/ | sed -e 's# *PRE \([^/]*\)/#\1#' | sort | while read -r PAIR
do
    [[ ! "${PAIR}" < "${START_PAIR}" ]] && [[ ! "${PAIR}" > "${END_PAIR}" ]] && echo "${PAIR}"
done
) )

start=`date +%s.%N`

parallel ./coalesce.sh {} $DATE $IGNORE_SUCCESS ::: ${PAIRS[@]}

end=`date +%s.%N`

runtime=$( echo "$end - $start" | bc -l )

echo "DONE. Runtime: ${runtime}"; echo "";