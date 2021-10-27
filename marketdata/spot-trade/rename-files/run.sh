#!/bin/bash

# ethereum=> SELECT MIN("exchangeHour"), MAX("exchangeHour") FROM trade_s3_objects;
#         min         |         max         
#---------------------+---------------------
# 2013-01-14 16:00:00 | 2021-10-12 06:00:00

START=$(date "+%s" -d "01/14/2013")
END=$(date "+%s" -d "10/12/2021")

until [[ $START > $END ]]; do 
    echo $(date -ud @${START})
    sh rename_day.sh $START
    START=$(($START + 86400))
done