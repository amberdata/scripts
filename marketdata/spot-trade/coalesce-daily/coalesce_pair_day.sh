#!/bin/bash

####################################################################################

START_PAIR=$1
END_PAIR=$2
IGNORE_SUCCESS=$3
START_DATE=$4
END_DATE=$5

####################################################################################

aws s3 ls s3://amberdata-marketdata/trade/ | sed -e 's# *PRE \([^/]*\)/#\1#' | sort | while read -r PAIR
do
  if [[ "${PAIR}" < "${START_PAIR}" ]];
  then
    continue
  fi

  if [[ "${PAIR}" > "${END_PAIR}" ]];
  then
    continue
  fi

  start=`date +%s.%N`

  DATES_PATH="s3://amberdata-marketdata/trade/${PAIR}/"

  DATES=( $(
    aws s3 ls "${DATES_PATH}" | awk '{ print $2 }' | sed 's/.$//' | sort -n -r | while read -r dt
    do
      [[ ! "${dt}" < "${START_DATE}" ]] && [[ ! "${dt}" > "${END_DATE}" ]] && echo "${dt}"
    done
  ) )

  if [ -z "${DATES}" ];
  then
    continue
  fi

  parallel ./coalesce.sh $PAIR {} $IGNORE_SUCCESS ::: ${DATES[@]}

  end=`date +%s.%N`

  runtime=$( echo "$end - $start" | bc -l )

  echo "DONE. Runtime: ${runtime}"; echo "";
done