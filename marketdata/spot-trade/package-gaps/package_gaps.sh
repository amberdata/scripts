#!/bin/bash

####################################################################################

DATA_DIR="./data"
OUTPUT_DIR="./output"

####################################################################################

aws s3 ls s3://amberdata-marketdata/spot-trade/ | sed -e 's# *PRE \([^/]*\)/#\1#' | sort | while read -r DATE
do
  echo "DATE: $DATE"

  DATE_PATH="s3://amberdata-marketdata/spot-trade/$DATE/"

  aws s3 cp "${DATE_PATH}" data/ --recursive --exclude "*" --include "*_GAPS"

  ls $DATA_DIR | while read -r PAIR
  do
    DATA_FILE_PATH="${DATA_DIR}/${PAIR}/_GAPS"
    PAIR_DIR = "${OUTPUT_DIR}/${PAIR}"
    mkdir -p PAIR_DIR
    OUTPUT_FILE_PATH="${PAIR_DIR}/${DATE}._GAPS"
    mv "${DATA_FILE_PATH}"  "${OUTPUT_FILE_PATH}"
  done

done
