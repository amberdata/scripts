
#!/bin/bash

####################################################################################

PAIR=$1
DATE=$2
IGNORE_SUCCESS=$3

S3_DIR="s3://amberdata-shar/spot-trade-v3/$DATE/$PAIR"

####################################################################################

echo "$PAIR $DATE $IGNORE_SUCCESS"

####################################################################################

if [[ "${IGNORE_SUCCESS}" == "false" ]];
then
  echo " CHECKING SUCCESS FLAG..."
  SUCCESS=$(aws s3 ls "${S3_DIR}/_SUCCESS" | wc -l)
  if [ "$SUCCESS" -ge 1 ];
  then
    echo " SUCCESS FILE PRESENT. SKIPPING..."
    exit
  else
    echo " SUCCESS FILE NOT PRESENT..."
  fi
fi

####################################################################################

TMP_DIR="/data/shar/coalesce_files/tmp/$DATE/$PAIR"
OUT="$TMP_DIR/_OUT"
ERR="$TMP_DIR/_ERR"
FILES="$TMP_DIR/s3.out.files"
S3_OUT="$TMP_DIR/s3.out"
DATA_DIR="$TMP_DIR/data"
OUTPUT_DIR="$TMP_DIR/output"

####################################################################################

mkdir -p "${OUTPUT_DIR}"
mkdir -p "${DATA_DIR}"
rm -f "${S3_OUT}" && rm -f "${OUT}" && rm -f "${ERR}"

####################################################################################

echo " DOWNLOADING FILES..."

start=`date +%s.%N`

SOURCE="s3://amberdata-marketdata/trade/$PAIR/$DATE/"
DEST="${DATA_DIR}/"

aws s3 sync "${SOURCE}" "${DEST}" >> "${OUT}" 2>> "${ERR}"

end=`date +%s.%N`

runtime=$( echo "$end - $start" | bc -l )

echo " Runtime: ${runtime}"; echo "";

####################################################################################

echo " COALESCING FILES..."

start=`date +%s.%N`

python3 coalesce_trade.py "${PAIR}" "${DATE}" "${DATA_DIR}" "${OUTPUT_DIR}" >> "${OUT}" 2>> "${ERR}"

echo "" >> "${OUT}" && echo "" >> "${ERR}"

end=`date +%s.%N`

runtime=$( echo "$end - $start" | bc -l )

echo " Runtime: ${runtime}"; echo "";

####################################################################################

echo " COPYING LOGS..."

start=`date +%s.%N`

cp "${OUT}" "${OUTPUT_DIR}"

ERR_LINES=$(cat "${ERR}" | wc -l )

[ "$ERR_LINES" -ge 2 ] && cp "${ERR}" "${OUTPUT_DIR}"

end=`date +%s.%N`

runtime=$( echo "$end - $start" | bc -l )

echo " Runtime: ${runtime}"; echo "";

####################################################################################

echo " SYNCING COALESCED FILES WITH S3..."

start=`date +%s.%N`

SOURCE=$OUTPUT_DIR
DEST="${S3_DIR}/"

aws s3 sync "${SOURCE}" "${DEST}" >> "${OUT}" 2>> "${ERR}"

end=`date +%s.%N`

runtime=$( echo "$end - $start" | bc -l )

echo " Runtime: ${runtime}"; echo "";

####################################################################################

echo " REMOVING DOWNLOADED AND COALESCED FILES..."

start=`date +%s.%N`

rm -rf "${TMP_DIR}"

end=`date +%s.%N`

runtime=$( echo "$end - $start" | bc -l )

echo " Runtime: ${runtime}"; echo "";

####################################################################################