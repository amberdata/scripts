
#!/bin/bash

####################################################################################

START_DATE=$1
END_DATE=$2

SOURCE="s3://amberdata-marketdata/spot-order-book-update"
DEST="s3://amberdata-repository/rentec/spot-order-book-update"

####################################################################################

DATES=( $(
  aws s3 ls "${SOURCE}/" | awk '{ print $2 }' | sed 's/.$//' | sort -n -r | while read -r dt
  do
    [[ ! "${dt}" < "${START_DATE}" ]] && [[ ! "${dt}" > "${END_DATE}" ]] && echo "${dt}"
  done
) )

parallel ./sync_dt.sh {} $SOURCE $DEST ::: ${DATES[@]}

####################################################################################