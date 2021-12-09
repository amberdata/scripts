# Scripts

A collection of utility scripts.

**marketdata/spot-trade/coalesce-daily/**

This folder contains scripts to download, coalesce, upload spot trade files.

Note: update the `TMP_DIR` variable in `marketdata/spot-trade/coalesce-daily/coalesce.sh` to preferred path.

```bash
# run the coalesce scripts for one pair/day.
marketdata/spot-trade/coalesce-daily/coalesce.sh btc_usd 2021-10-01
# run the scripts for a pair-date range and check for success flags.
# In this example we run all pairs between & including `btc_usd` and `eth_usd` and all
# dates between & including `2021-10-01` and `2021-10-10`. We check _SUCCESS flags (Skip Check Success = false) to detect pair/days we've already run.
marketdata/spot-trade/coalesce-daily/coalesce_pair_day.sh btc_usd eth_usd false 2021-10-01 2021-10-10
```