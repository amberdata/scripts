#!/bin/bash

####################################################################################

DATA_DIR=$(echo "$(pwd)/data")
rm -rf "${DATA_DIR}"

OUTPUT_DIR=$(echo "$(pwd)/output")
OUT="_OUT"
ERR="_ERR"
rm -f $OUT && rm -f $ERR

####################################################################################

aws s3 ls s3://amberdata-marketdata/spot-trade/ | sed -e 's# *PRE \([^/]*\)/#\1#' | sort -n -r | while read -r DATE
do

  mkdir -p "${DATA_DIR}"

  echo "DATE: $DATE"

  DATE_PATH="s3://amberdata-marketdata/spot-trade/$DATE/"

  echo " DOWNLOADING FILES FOR $DATE"

  start=`date +%s.%N`

  aws s3 cp "${DATE_PATH}" "${DATA_DIR}" --recursive --exclude "*" --include "*_GAPS" >> "${OUT}" 2>> "${ERR}"

  end=`date +%s.%N`

  runtime=$( echo "$end - $start" | bc -l )

  echo " Runtime: ${runtime}"; echo "";

  echo " MOVING FILES FOR DATE $DATE"

  start=`date +%s.%N`

  ls $DATA_DIR | while read -r PAIR
  do

    SOURCE_GAP_FILE_PATH="${DATA_DIR}/${PAIR}/_GAPS"
    DEST="${OUTPUT_DIR}/${PAIR}"

    mkdir -p "${DEST}"
    
    DEST_GAP_FILE_PATH="${DEST}/${DATE}._GAPS"
    mv "${SOURCE_GAP_FILE_PATH}" "${DEST_GAP_FILE_PATH}"
  done

  end=`date +%s.%N`

  runtime=$( echo "$end - $start" | bc -l )

  echo " Runtime: ${runtime}"; echo "";

  rm -rf "${DATA_DIR}"

done
