#!/bin/bash

####################################################################################

OUTPUT_DIR="./packaged_gaps"

cat s3.out.files.gaps | while read -r GAP_FILE
do
    DATE=$( echo $GAP_FILE | awk -F/ '{print $5}' )
    PAIR=$( echo $GAP_FILE | awk -F/ '{print $6}' )

    echo $GAP_FILE $DATE $PAIR

    PAIR_DIR="${OUTPUT_DIR}/${PAIR}"
    PAIR_FILE_PATH="${PAIR_DIR}/${DATE}"

    mkdir -p "${PAIR_DIR}"

    aws s3 cp ${GAP_FILE} ${PAIR_FILE_PATH}
done

