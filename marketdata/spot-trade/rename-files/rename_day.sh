#!/bin/bash

# Start as a unix timestamp.
# ========================================================================================
START=$1

# Start as a date formatted as mm/dd/YYYY i.e. 02/03/2021
# ========================================================================================
#START=$(date "+%s" -d "${1}")

END=$(($START + 86400))

# # Below a test to verify query works.
# # ========================================================================================
# echo  "SELECT" \
#         "regexp_replace(" \
#           "\"objectKey\"," \
#           "'([0-9]{2}):([0-9]{2}):([0-9]{2})', '\1-\2-\3'" \
#         ")" \
#       "FROM trade_s3_objects" \
#       "WHERE \"exchangeHour\" >= TO_TIMESTAMP(${START})::TIMESTAMP WITHOUT TIME ZONE" \
#         "AND \"exchangeHour\" < TO_TIMESTAMP(${END})::TIMESTAMP WITHOUT TIME ZONE;" \
#       | psql -h ethereum-mainnet-db.amberdata.internal -U spruisken -d ethereum -p 5432 # comment this line for dry-run mode.

# Below the actual update query to modify database rows.
# ========================================================================================
echo  "UPDATE \"public\".trade_s3_objects" \
      "SET \"objectKey\" = regexp_replace(\"objectKey\", '([0-9]{2}):([0-9]{2}):([0-9]{2})', '\1-\2-\3')" \
      "WHERE \"exchangeHour\" >= TO_TIMESTAMP(${START})::TIMESTAMP WITHOUT TIME ZONE" \
        "AND \"exchangeHour\" < TO_TIMESTAMP(${END})::TIMESTAMP WITHOUT TIME ZONE;" \
      | psql -h ethereum-mainnet-db.amberdata.internal -U spruisken -d ethereum -p 5432 # comment this line for dry-run mode.