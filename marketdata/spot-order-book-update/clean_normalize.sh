#!/bin/bash                                                                                                                                 

time sed -e 's/.* \(s3.*\) to .*/\1/' s3.out > s3.out.files

grep "_ERR" s3.out.files > s3.out.files._err
grep "_OUT" s3.out.files > s3.out.files._out
grep "/out" s3.out.files > s3.out.files.out


aws s3 cp s3.out.files.out s3://amberdata-shar/spot-order-book-update-daily/_OUT_FILES
aws s3 cp s3.out.files._out s3://amberdata-shar/spot-order-book-update-daily/_OUT
aws s3 cp s3.out.files._err s3://amberdata-shar/spot-order-book-update-daily/_ERR

# Remove 18c_btc, result of using a buggy coalesce script                                                                                   
cat s3.out.files._err | grep 18c_btc > s3.out.files._err.18c_btc
cat s3.out.files._err.18c_btc | parallel aws s3 rm {}

# _ERR files except 18c_btc
cat s3.out.files._err | grep -v 18c_btc > s3.out.files._err.norm

aws s3 cp s3.out.files._err.norm s3://amberdata-shar/spot-order-book-update-daily/_ERR