
#!/bin/bash

####################################################################################

START_DATE=$1
END_DATE=$2

OUT=s3.out
ERR=s3.err

FILES=s3.out.files

CANDIDATES=candidates

# time aws s3 cp s3://amberdata-marketdata/order_book_snapshot/ . --recursive --dryrun > $OUT 2> $ERR

time sed -e 's/.* \(s3.*\) to .*/\1/' $OUT > $FILES

while IFS="" read -r LINE || [ -n "$LINE" ]
do
  DATE=$(echo $LINE | grep -Eo '[[:digit:]]{4}-[[:digit:]]{2}-[[:digit:]]{2}')

  if [[ "${DATE}" < "${START_DATE}" ]];
  then
    continue
  fi

  if [[ "${DATE}" > "${END_DATE}" ]];
  then
    continue
  fi

  echo $LINE >> CANDIDATES

done < $FILES


####################################################################################