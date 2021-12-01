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
        "fileName": last_modified,
    }

def try_load_json_file(file, last_modified):
  lines = file.readlines()
  result = []
  for line in lines:
    try:
      result.append({**json.loads(line.strip()), "fileName": last_modified})
    except Exception as e:
      print(f"line: {line}")
      print(f"Failures in try_load_json_file: {e}", file=sys.stderr)
  return result

def get_file_name(file_path):
  file_name = file_path.split('/')[-1].split('.')[0]
  return file_name
  
def get_grouped_lines(_file_paths):

    # each line is a json
    default = [file_path for file_path in _file_paths if not is_csv_gzip(file_path)]
    default_files = [(open(file_path, "r"), get_file_name(file_path)) for file_path in default]
    default_grouped_lines = [try_load_json_file(file, last_modified) for (file, last_modified) in default_files]

    #gzipped csv files
    gzipped = [file_path for file_path in _file_paths if is_csv_gzip(file_path)]
    gzipped_files = [(gzip.open(file_path, 'rb'), get_file_name(file_path))  for file_path in gzipped]
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
            lines.append(line)

    write_lines(_exchange, lines)

def write_lines(_exchange, _lines):

    o_path = f"{OUTPUT_DIR}/{_exchange}"

    with open(o_path, "w") as o:
        for i in range(0, len(_lines)):
            item = _lines[i]
            o.write(f"{json.dumps(item, separators=(',', ':'))}\n")

def get_exchanges():

    files = glob.glob(f"{DATA_DIR}/*/*")

    return sorted(list(set([file.split("/")[-1].split("-")[0] for file in files])))

if __name__ == "__main__":

    metadata = []

    exchanges = get_exchanges()

    for exchange in exchanges:

        files = glob.glob(f"{DATA_DIR}/*/{exchange}*")

        process_exchange(exchange, files)

    with open(f"{OUTPUT_DIR}/_SUCCESS", "w") as f:
        f.write(f"coalesce completed: {int(time.time())}\n")

