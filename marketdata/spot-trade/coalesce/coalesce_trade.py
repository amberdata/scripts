import os
import sys
import glob
import json
import gzip
import time

PAIR = sys.argv[1]
DATE = sys.argv[2]
DATA_DIR = sys.argv[3]
OUTPUT_DIR=sys.argv[4]

gaps = []

def is_csv_gzip(_file_path):
    return _file_path.endswith("csv.gz")

def convert_line_from_csv_gz(_line):
    [timestamp, timestampNanoseconds, tradeId, price, size, isBuySide] = _line.decode('UTF-8').strip().split(',')
    return {
        "timestamp": int(timestamp),
        "timestampNanoseconds": int(timestampNanoseconds),
        "tradeId": int(tradeId),
        "size": float(size),
        "price": float(price),
        "isBuySide": bool(isBuySide),
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


def process_exchange(_exchange, _file_paths):

    grouped_lines = get_grouped_lines(_file_paths)

    lines = []

    for group in grouped_lines:
        for line in group:
            line["exchange"] = _exchange
            line["pair"] = PAIR
            lines.append(line)

    lines.sort(key = lambda item : get_key(item))

    return process_lines(_exchange, PAIR, lines)

def process_lines(_exchange, _pair, _lines):
    
    exchange_metadata = {
        "exchange": _exchange,
        "pair": _pair,
        # Defaults that will be updated as we process lines:
        "num_records": 0,
        "min_timestamp": sys.maxsize,
        "min_timestampNanoseconds": sys.maxsize,
        "max_timestampNanoseconds": -1,
        "max_timestamp": -1,
        "min_sell_price": sys.maxsize,
        "max_sell_price": -1,
        "min_buy_price": sys.maxsize,
        "max_buy_price": -1,
        "file_size": None,
        "min_trade_id": sys.maxsize,
        "max_trade_id": -1,
    }

    # initalize previous.
    prev_item = None; prev_item_key = None; prev_item_value = None;

    o_path = f"{OUTPUT_DIR}/{_exchange}"

    with open(o_path, "w") as o:
        for i in range(0, len(_lines)):

            item = _lines[i]; item_key = get_key(item); item_value = get_value(item);

            if prev_item_key == item_key:
                if prev_item_value != item_value:
                    print(f"ERROR: duplicate item {item_key} with different values {prev_item_value} {item_value}", file=sys.stderr)
                else:
                    print(f"duplicate item {item_key}")
            else:
                o.write(f"{json.dumps(item, separators=(',', ':'))}\n")
                # increment num_records only for non-duplicates
                exchange_metadata["num_records"] += 1

            find_gap(prev_item, item, _exchange)
            update_metadata(item, exchange_metadata)

            prev_item = item; prev_item_key = item_key; prev_item_value = item_value;

    exchange_metadata["file_size"] = os.path.getsize(o_path)

    return exchange_metadata

def find_gap(prev_item, item, _exchange):
    if prev_item and "tradeId" in prev_item and "tradeId" in item:
        diff = item["tradeId"] - prev_item["tradeId"]
        if diff > 1:
            gaps.append([
                item["pair"],
                _exchange,
                prev_item["timestamp"],
                item["timestamp"],
                prev_item["tradeId"],
                item["tradeId"],
                diff,
            ])

def get_key(item):
    item_keys = ["pair", "exchange", "timestamp", "timestampNanoseconds", "tradeId", "isBuySide"]

    for item_key in item_keys:
        if item_key not in item:
            print(f"ERROR: missing price or size {item}", file=sys.stderr)
            break

    return tuple([item[item_key] if item_key in item else None for item_key in item_keys])


def get_value(item):
    item_values = ["price", "size"]

    for item_value in item_values:
        if item_value not in item:
            print(f"ERROR: missing price or size {item}", file=sys.stderr)
            break

    return tuple([item[item_value] if item_value in item else None for item_value in item_values])

def update_metadata(item, _exchange_metadata):
    try:
        if item["isBuySide"]:
            _exchange_metadata["max_buy_price"] = max(item["price"], _exchange_metadata["max_buy_price"])
            _exchange_metadata["min_buy_price"] = min(item["price"], _exchange_metadata["max_buy_price"])
            
        else:
            _exchange_metadata["max_sell_price"] = max(item["price"], _exchange_metadata["max_sell_price"])
            _exchange_metadata["min_sell_price"] = min(item["price"], _exchange_metadata["max_sell_price"])

        
        if item["timestamp"] > _exchange_metadata["max_timestamp"]:
            _exchange_metadata["max_timestamp"] = item["timestamp"]
            _exchange_metadata["max_timestampNanoseconds"] = item["timestampNanoseconds"]
        elif item["timestamp"] == _exchange_metadata["max_timestamp"]:
            _exchange_metadata["max_timestampNanoseconds"] = max(item["timestampNanoseconds"], _exchange_metadata["max_timestampNanoseconds"])           

        if item["timestamp"] < _exchange_metadata["min_timestamp"]:
            _exchange_metadata["min_timestamp"] = item["timestamp"]
            _exchange_metadata["min_timestampNanoseconds"] = item["timestampNanoseconds"]
        elif item["timestamp"] == _exchange_metadata["min_timestamp"]:
            _exchange_metadata["min_timestampNanoseconds"] = min(item["timestampNanoseconds"], _exchange_metadata["min_timestampNanoseconds"])

    except KeyError(e):
        print(f"missing price or isBuySide {item}", file=sys.stderr)

    try:
        _exchange_metadata["max_trade_id"] = max(item["tradeId"], _exchange_metadata["max_trade_id"])
        _exchange_metadata["min_trade_id"] = min(item["tradeId"], _exchange_metadata["min_trade_id"])
    except:
        pass

def get_exchanges():

    files = glob.glob(f"{DATA_DIR}/*/*")

    return sorted(list(set([file.split("/")[-1].split("-")[0] for file in files])))

if __name__ == "__main__":

    metadata = []

    exchanges = get_exchanges()

    for exchange in exchanges:

        files = glob.glob(f"{DATA_DIR}/*/{exchange}*")

        processed_exchange_metadata = process_exchange(exchange, files)

        metadata.append(processed_exchange_metadata)

    if len(gaps) > 0:
        # Sort gaps by exchange, gap size (DESC)
        gaps.sort(key = lambda item: (item[1], -1 * item[6]))
        with open(f"{OUTPUT_DIR}/_GAPS", "w") as g:
            for gap in gaps:
                _line = ",".join([str(x) for x in gap]) + "\n"
                g.write(_line)

    with open(f"{OUTPUT_DIR}/_SUCCESS", "w") as f:
        last_updated = int(time.time())
        for data in metadata:
            data["last_updated"] = last_updated
        f.write(json.dumps(metadata, separators=(',', ':')))
