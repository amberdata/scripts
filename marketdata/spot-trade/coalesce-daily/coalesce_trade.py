import os
import sys
import glob
import json
import gzip
import time
from datetime import datetime

PAIR = sys.argv[1]
DATE = sys.argv[2]
DATA_DIR = sys.argv[3]
OUTPUT_DIR=sys.argv[4]

# 2021-10-02T00:00:00 UTC
TIMESTAMP_CUTOFF = 1630540800000

gaps = []

def is_csv_gzip(_file_path):
    return _file_path.endswith("csv.gz")

def try_load_csv_gz_file(file, last_modified):
  result = []
  for line in file.readlines():
    try:
      result.append(convert_line_from_csv_gz(line, last_modified))
    except Exception as e:
      print(f"Failures in try_load_csv_gz_file: {e}", file=sys.stderr)
  return result


def convert_line_from_csv_gz(_line, last_modified):
    [timestamp, timestampNanoseconds, tradeId, price, size, isBuySide] = _line.decode('UTF-8').strip().split(',')
    return {
        "timestamp": int(timestamp),
        "timestampNanoseconds": int(timestampNanoseconds),
        "tradeId": int(tradeId),
        "size": float(size),
        "price": float(price),
        "isBuySide": bool(isBuySide),
        "lastModified": last_modified,
    }

def try_load_json_file(file, last_modified):
  lines = file.readlines()
  result = []
  for line in lines:
    try:
      result.append({**json.loads(line.strip()), "lastModified": last_modified})
    except Exception as e:
      print(f"Failures in try_load_json_file: {e}", file=sys.stderr)
  return result

def get_last_modified(file_path):
  lastModifiedStr = "-".join(file_path.split('/')[-1].split('.')[0].split('-')[1:])
  try:
    dt = datetime.strptime(lastModifiedStr, "%Y-%m-%d@%H-%M-%S")
    return time.mktime(dt.timetuple())
  except Exception:
    return 0

def get_corrected_time(timestamp, timestampNanoseconds):
  if timestamp < TIMESTAMP_CUTOFF and timestamp % 1000 == 0 and timestampNanoseconds >= 1000:
    microseconds = timestampNanoseconds // 1000
    _timestamp = timestamp + microseconds
    _timestampNanoseconds = timestampNanoseconds - microseconds * 1000
    return (_timestamp, _timestampNanoseconds)
  return (timestamp, timestampNanoseconds)
  
  

def get_grouped_lines(_file_paths):

    # each line is a json
    default = [file_path for file_path in _file_paths if not is_csv_gzip(file_path)]
    default_files = [(open(file_path, "r"), get_last_modified(file_path)) for file_path in default]
    default_grouped_lines = [try_load_json_file(file, last_modified) for (file, last_modified) in default_files]

    #gzipped csv files
    gzipped = [file_path for file_path in _file_paths if is_csv_gzip(file_path)]
    gzipped_files = [(gzip.open(file_path, 'rb'), get_last_modified(file_path))  for file_path in gzipped]
    gzipped_grouped_lines = [try_load_csv_gz_file(file, last_modified) for (file, last_modified) in gzipped_files]

    for (file, _) in default_files + gzipped_files:
        file.close()

    return default_grouped_lines + gzipped_grouped_lines

def process_exchange(_exchange, _file_paths):

    grouped_lines = get_grouped_lines(_file_paths)

    lines = []

    for group in grouped_lines:
        for line in group:
            line["exchange"] = _exchange
            line["pair"] = PAIR
            try:
             (line["timestamp"], line["timestampNanoseconds"]) = get_corrected_time(line["timestamp"], line["timestampNanoseconds"])
            except Exception as e:
              pass
            lines.append(line)

    lines.sort(key = lambda item : get_sort_key(item))

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

            item.pop("lastModified")

            if prev_item_key == item_key:
                if prev_item_value != item_value:
                    print(f"DUPLICATE DIFFERENT item {item_key} with different values {prev_item_value} {item_value}", file=sys.stderr)
                else:
                    print(f"DUPLICATE SAME item {item_key}")
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
    item_keys = [("pair", ""), ("exchange", ""), ("timestamp", 0), ("timestampNanoseconds", 0), ("tradeId", 0), ("isBuySide", True)]

    for (item_key, _) in item_keys:
        if item_key not in item:
            print(f"ERROR: missing price or size {item}", file=sys.stderr)
            break

    return tuple([item[item_key] if item_key in item and item[item_key] else default for (item_key, default) in item_keys])

def get_sort_key(item):
  result = tuple(list(get_key(item)) + [-1 * item["lastModified"]])
  print(f"RESULT {result}")
  return result

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

    except Exception as e:
        print(f"missing price or isBuySide {item} {e}", file=sys.stderr)

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

