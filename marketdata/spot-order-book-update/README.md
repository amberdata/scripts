# Coalesce Order Book Update

This sub-directory contains several scripts that coalesce order book update data. Coalescing works as follows:
- Download all files for one pair / day i.e. s3://amberdata-marketdata/order_book_update/[PAIR]/[DAY]/
- For each file, parse each line and write it out to the coalesced output file. Store output in _OUT and any parse failures in _ERR
- Write coalesced, _OUT, _ERR to S3

The following coalesces files and writes the output to S3 for `btc_usd` on `2021-10-01`:

```
./coalesce.sh btc_usd 2021-10-01
```

To coalesce multiple pair / days in parallel use `coalesce_pair_day.sh`:

```bash
./coalesce_pair_day.sh \
    0000 \ # start pair
    zzzzzz \ # end pair (inclusive)
    false \ # Whether to ignore previous coalesced files and overwrite them
    2010-01-01 \ # start day
    2030-01-01 # end day (inclusive)
```

Our order book update dataset is large. Coalescing all files will take several days (3-4) on 4 machines with 32 cores.


