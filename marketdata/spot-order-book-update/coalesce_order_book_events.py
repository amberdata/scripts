import os
import sys
import glob
import json
import gzip
import time

# PAIR="btc_usd"
# DATE="2021-10-01"
# DATA_DIR="tmp/data"
# OUTPUT_DIR="tmp/output"

PAIR = sys.argv[1]
DATE = sys.argv[2]
DATA_DIR = sys.argv[3]
OUTPUT_DIR=sys.argv[4]

#############################################################################

# Metadata
# min, max sequence number from metadata.sequence ? isBid true / false
# min / max price isBid true / false
# key: exchange, pair, 

#############################################################################

def is_csv_gzip(_file_path):
    return _file_path.endswith("csv.gz")

def convert_line_from_csv_gz(_line):
    [exchangeTimestamp, exchangeTimestampNanoseconds, isBid, data, receivedTimestamp, receivedTimestampNanoseconds] = _line.decode('UTF-8').strip().split(';')
    return {
        "exchangeTimestamp": int(exchangeTimestamp),
        "exchangeTimestampNanoseconds": int(exchangeTimestampNanoseconds),
        "isBid": bool(isBid),
        "data": json.loads(data),
        "receivedTimestamp": int(receivedTimestamp),
        "receivedTimestampNanoseconds": int(receivedTimestampNanoseconds),
        "metadata": None,
    }

def get_grouped_lines(_file_paths):

    # each line is a json
    default = [file_path for file_path in _file_paths if not is_csv_gzip(file_path)]
    default_files = [open(file_path, "r") for file_path in default]
    default_grouped_lines = [[json.loads(line.strip()) for line in file.readlines()] for file in default_files]

    #gzipped csv files
    gzipped = [file_path for file_path in _file_paths if is_csv_gzip(file_path)]
    gzipped_files = [gzip.open(file_path, 'rb') for file_path in gzipped]
    gzipped_grouped_lines = [[convert_line_from_csv_gz(line) for line in file.readlines()] for file in gzipped_files]

    for file in default_files + gzipped_files:
        file.close()

    return default_grouped_lines + gzipped_grouped_lines


def process_exchange(_exchange, hr, _file_paths):

    grouped_lines = get_grouped_lines(_file_paths)

    lines = []

    for group in grouped_lines:
        for line in group:
            line["exchange"] = _exchange
            line["pair"] = PAIR
            lines.append(line)

    lines.sort(key = lambda item : get_key(item))

    return process_lines(_exchange, hr, PAIR, lines)

def process_lines(_exchange, hr, _pair, _lines):
    
    exchange_metadata = {
        "exchange": _exchange,
        "hour": hr,
        "pair": _pair,
        # Defaults that will be updated as we process lines:
        "num_records": 0,
        "min_exchangeTimestamp": sys.maxsize,
        "max_exchangeTimestamp": -1,
        "min_exchangeTimestampNanoseconds": sys.maxsize,
        "max_exchangeTimestampNanoseconds": -1,
        "min_receivedTimestamp": sys.maxsize,
        "max_receivedTimestamp": -1,
        "min_receivedTimestampNanoseconds": sys.maxsize,
        "max_receivedTimestampNanoseconds": -1,
        "min_bid_price": sys.maxsize,
        "max_bid_price": -1,
        "min_bid_volume": sys.maxsize,
        "max_bid_volume": -1,
        "min_bid_trades": sys.maxsize,
        "max_bid_trades": -1,
        "min_ask_price": sys.maxsize,
        "max_ask_price": -1,
        "min_ask_volume": sys.maxsize,
        "max_ask_volume": -1,
        "min_ask_trades": sys.maxsize,
        "max_ask_trades": -1,
        "file_size": None,
        "min_sequence_id": sys.maxsize,
        "max_sequence_id": -1,
    }

    # initalize previous.
    prev_item = None; prev_item_key = None; prev_item_value = None;

    o_path = f"{OUTPUT_DIR}/{_exchange}-{hr}"

    with open(o_path, "w") as o:
        for i in range(0, len(_lines)):

            item = _lines[i]; item_key = get_key(item); item_value = get_value(item);

            if prev_item_key == item_key:
                pass
                if prev_item_value != item_value:
                    print(f"ERROR: duplicate item {item_key} with different values", file=sys.stderr)
                else:
                    print(f"DEBUG: duplicate item {item_key}", file=sys.stderr)
            else:
                o.write(f"{json.dumps(item, separators=(',', ':'))}\n")
                # increment num_records only for non-duplicates
                exchange_metadata["num_records"] += 1

            update_metadata(item, exchange_metadata)

            prev_item = item; prev_item_key = item_key; prev_item_value = item_value;

    exchange_metadata["file_size"] = os.path.getsize(o_path)

    return sanitize_metadata(exchange_metadata)

def sanitize_metadata(_metadata):
    return {k:v for (k,v) in _metadata.items() if v != -1 and v != sys.maxsize}

def get_key(item):
    item_keys = ["pair", "exchange", "exchangeTimestamp", "exchangeTimestampNanoseconds", "isBid"]

    for item_key in item_keys:
        if item_key not in item:
            print(f"ERROR: missing price or size {item}", file=sys.stderr)
            break

    return tuple([item[item_key] if item_key in item else None for item_key in item_keys])


def get_value(item):

    if "data" not in item:
        print(f"ERROR: missing price or size {item}", file=sys.stderr)
        return tuple([None])

    item_values = ["data"]

    for item_value in item_values:
        if item_value not in item:
            print(f"ERROR: missing price or size {item}", file=sys.stderr)
            break

    return tuple([item[item_value] if item_value in item else None for item_value in item_values])

def get_min_max_price(item):
    return (min([float(e[0]) for e in item]), max([float(e[0]) for e in item]))

def get_min_max_volume(item):
    return (min([float(e[1]) for e in item]), max([float(e[1]) for e in item]))

def get_min_max_trades(item):
    processed = [e for e in item if len(e) == 3 and e[2]]
    if len(processed) == 0:
        return (sys.maxsize, -1)
    return (min([float(e[2]) for e in item]), max([float(e[2]) for e in item]))

def update_metadata(item, _exchange_metadata):
    try:
        if "data" in item:
            data = item["data"]
            if item["isBid"]:
                _exchange_metadata["max_bid_price"] = max(get_min_max_price(data)[1], _exchange_metadata["max_bid_price"])
                _exchange_metadata["min_bid_price"] = min(get_min_max_price(data)[0], _exchange_metadata["min_bid_price"])
                _exchange_metadata["max_bid_volume"] = max(get_min_max_volume(data)[1], _exchange_metadata["max_bid_volume"])
                _exchange_metadata["min_bid_volume"] = min(get_min_max_volume(data)[0], _exchange_metadata["min_bid_volume"])
                _exchange_metadata["max_bid_trades"] = max(get_min_max_trades(data)[1], _exchange_metadata["max_bid_trades"])
                _exchange_metadata["min_bid_trades"] = min(get_min_max_trades(data)[0], _exchange_metadata["min_bid_trades"])                        
                
            else:
                _exchange_metadata["max_ask_price"] = max(get_min_max_price(data)[1], _exchange_metadata["max_ask_price"])
                _exchange_metadata["min_ask_price"] = min(get_min_max_price(data)[0], _exchange_metadata["min_ask_price"])
                _exchange_metadata["max_ask_volume"] = max(get_min_max_volume(data)[1], _exchange_metadata["max_ask_volume"])
                _exchange_metadata["min_ask_volume"] = min(get_min_max_volume(data)[0], _exchange_metadata["min_ask_volume"])
                _exchange_metadata["max_ask_trades"] = max(get_min_max_trades(data)[1], _exchange_metadata["max_ask_trades"])
                _exchange_metadata["min_ask_trades"] = min(get_min_max_trades(data)[0], _exchange_metadata["min_ask_trades"])       

        
        if item["exchangeTimestamp"] > _exchange_metadata["max_exchangeTimestamp"]:
            _exchange_metadata["max_exchangeTimestamp"] = item["exchangeTimestamp"]
            _exchange_metadata["max_exchangeTimestampNanoseconds"] = item["exchangeTimestampNanoseconds"]
        elif item["exchangeTimestamp"] == _exchange_metadata["max_exchangeTimestamp"]:
            _exchange_metadata["max_exchangeTimestampNanoseconds"] = max(item["exchangeTimestampNanoseconds"], _exchange_metadata["max_exchangeTimestampNanoseconds"])           

        if item["exchangeTimestamp"] < _exchange_metadata["min_exchangeTimestamp"]:
            _exchange_metadata["min_exchangeTimestamp"] = item["exchangeTimestamp"]
            _exchange_metadata["min_exchangeTimestampNanoseconds"] = item["exchangeTimestampNanoseconds"]
        elif item["exchangeTimestamp"] == _exchange_metadata["min_exchangeTimestamp"]:
            _exchange_metadata["min_exchangeTimestampNanoseconds"] = min(item["exchangeTimestampNanoseconds"], _exchange_metadata["min_exchangeTimestampNanoseconds"])

        if item["receivedTimestamp"] > _exchange_metadata["max_receivedTimestamp"]:
            _exchange_metadata["max_receivedTimestamp"] = item["receivedTimestamp"]
            _exchange_metadata["max_receivedTimestampNanoseconds"] = item["receivedTimestampNanoseconds"]
        elif item["receivedTimestamp"] == _exchange_metadata["max_receivedTimestamp"]:
            _exchange_metadata["max_receivedTimestampNanoseconds"] = max(item["receivedTimestampNanoseconds"], _exchange_metadata["max_receivedTimestampNanoseconds"])           

        if item["receivedTimestamp"] < _exchange_metadata["min_receivedTimestamp"]:
            _exchange_metadata["min_receivedTimestamp"] = item["receivedTimestamp"]
            _exchange_metadata["min_receivedTimestampNanoseconds"] = item["receivedTimestampNanoseconds"]
        elif item["receivedTimestamp"] == _exchange_metadata["min_receivedTimestamp"]:
            _exchange_metadata["min_receivedTimestampNanoseconds"] = min(item["receivedTimestampNanoseconds"], _exchange_metadata["min_receivedTimestampNanoseconds"])         

    except Exception:
        print(f"missing fields {item}", file=sys.stderr)

    try:
        _exchange_metadata["max_sequence_id"] = max(item["metadata"]["sequence"], _exchange_metadata["max_sequence_id"])
        _exchange_metadata["min_sequence_id"] = min(item["metadata"]["sequence"], _exchange_metadata["min_sequence_id"])
    except Exception:
        pass


def get_str_hour(hr):
    if hr < 10:
        return f"0{hr}"
    return str(hr)

def get_exchanges(str_hr):

    files = glob.glob(f"{DATA_DIR}/{str_hr}/*")

    return sorted(list(set([file.split("/")[-1].split("-")[0] for file in files])))

if __name__ == "__main__":

    metadata = []

    for hr in range(24):

        str_hr = get_str_hour(hr)

        exchanges = get_exchanges(str_hr)

        print(f"HOUR: {str_hr} EXCHANGES: {exchanges}")

        for exchange in exchanges:

            files = glob.glob(f"{DATA_DIR}/{str_hr}/{exchange}*")

            if not files:
                continue

            processed_exchange_metadata = process_exchange(exchange, str_hr, files)

            metadata.append(processed_exchange_metadata)

    with open(f"{OUTPUT_DIR}/_SUCCESS", "w") as f:
        last_updated = int(time.time())
        for data in metadata:
            data["last_updated"] = last_updated
        f.write(json.dumps(metadata, separators=(',', ':')))
