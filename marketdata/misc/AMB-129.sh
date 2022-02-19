
#!/bin/bash

####################################################################################

# Copy corrected data on local machine. Do this once.
#aws s3 sync "s3://amberdata-shar/AMB-129/" "/data4/shar/AMB-129/"

python3 AMB-129.cp.py "/data4/shar/AMB-129" > cmd.cp.out

parallel < cmd.cp.out

aws s3 cp s3://amberdata-marketdata/trade/ . --recursive --dryrun > s3.out 2> s3.err
time sed -e 's/.* \(s3.*\) to .*/\1/' s3.out > s3.out.files

python3 AMB-129.cp.py s3.out.files > cmd.mv.out

parallel < cmd.mv.out

# cmd.cp.out, cmd.mv.out can be parsed to retrieve the new / old files names and add / delete these from
# `trade_s3_objects`





