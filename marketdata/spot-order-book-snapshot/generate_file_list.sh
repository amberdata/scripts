
#!/bin/bash

####################################################################################

START_DATE=$1
END_DATE=$2

aws s3 cp s3://amberdata-marketdata/order_book_snapshot/ . --recursive --dryrun > s3.out 2> s3.err

time sed -e 's/.* \(s3.*\) to .*/\1/' s3.out > s3.out.files



####################################################################################