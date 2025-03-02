### RUN PARAMS

``./coalesce_pair_day.sh {START_PAIR} {END_PAIR} {VERIFY_IF_SUCCESS_FILE} {START_DATE} {END_DATE} {SOURCE_FILE} {DESTINATION_FILE}
``

### EXAMPLE

``./coalesce_pair_day.sh 10000NFTUSDT 10000NFTUSDT true 2022-01-27 2022-01-27 s3://amberdata-marketdata/futures_order_book_event s3://amberdata-efra/futures_order_book_event``
