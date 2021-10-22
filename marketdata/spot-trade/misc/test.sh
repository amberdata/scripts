
#!/bin/bash

####################################################################################

PAIR=$1
DATE=$2

echo "$PAIR $DATE"

S3_DIR="s3://amberdata-marketdata/spot-trade/${DATE}/${PAIR}"

echo $S3_DIR

####################################################################################

TMP_DIR="/data/shar/coalesce_files/tmp/${DATE}/${PAIR}"
OUT="${TMP_DIR}/_OUT"
ERR="${TMP_DIR}/_ERR"
FILES="${TMP_DIR}/s3.out.files"
S3_OUT="${TMP_DIR}/s3.out"
DATA_DIR="${TMP_DIR}/data"
OUTPUT_DIR="${TMP_DIR}/output"

echo $TMP_DIR
echo $OUT
echo $ERR
echo $FILES
echo $S3_OUT
echo $DATA_DIR
echo $OUTPUT_DIR

####################################################################################

# mkdir -p $OUTPUT_DIR
# mkdir -p $DATA_DIR
# rm -f $S3_OUT && rm -f $OUT && rm -f $ERR

####################################################################################