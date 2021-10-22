#!/bin/bash

####################################################################################

IGNORE_SUCCESS=$1
START_PAIR=$2
END_PAIR=$3

####################################################################################

aws s3 ls s3://amberdata-marketdata/spot-trade/ | sed -e 's# *PRE \([^/]*\)/#\1#' | sort | while read -r DATE
do
  echo "DATE: $DATE"

  aws s3 rm --recursive "s3://amberdata-marketdata/spot-trade/$DATE/hydro_protocol_btc/"
  aws s3 rm --recursive "s3://amberdata-marketdata/spot-trade/$DATE/hydro_protocol_eth/"
  aws s3 rm --recursive "s3://amberdata-marketdata/spot-trade/$DATE/hydro_protocol_usd/"
done