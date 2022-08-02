import glob
import json
import gzip
import time
import sys
import re


def is_csv_gzip(_file_path):
    return _file_path.endswith("csv.gz")


def add_exchange(file_path):
    try:
        matches = list(re.finditer(r"((?:.(?!\/))+$)", file_path))
        file_name = matches[0].group()
        return re.match(r"\/(.*?)-", file_name).group(1)
    except Exception as e:
        print(f"file: {file_path}", file=sys.stderr)


def convert_line_from_csv_gz(_line, file_path):
    # TODO: Verify gz files in futures
    [timestamp, timestampNanoseconds, tradeId, price, size, isBuySide] = _line.decode('UTF-8').strip().split(',')
    return {
        "exchange": add_exchange(file_path),
        "timestamp": int(timestamp),
        "timestampNanoseconds": int(timestampNanoseconds),
        "tradeId": int(tradeId),
        "size": float(size),
        "price": float(price),
        "isBuySide": bool(isBuySide),
        "filePath": file_path,
    }


def process_pair_day(output_path, file_paths):

    o_path = f"{output_path}/out"

    with open(o_path, "w") as o:

      json_paths = [file_path for file_path in file_paths if not is_csv_gzip(file_path)]

      for json_path in json_paths:
        with open(json_path, 'r') as f:
          for i, line in enumerate(f):
            try:
              result = {**json.loads(line.strip()), "filePath": json_path}
              o.write(f"{json.dumps(result, separators=(',', ':'))}\n")
            except Exception as e:
              print(f"file: {json_path}", file=sys.stderr)
              print(f"line_index: {i}", file=sys.stderr)
              print(f"line: {line}", file=sys.stderr)

      csv_gz_paths = [file_path for file_path in file_paths if is_csv_gzip(file_path)]

      for csv_gz_path in csv_gz_paths:
        with gzip.open(csv_gz_path, 'rb') as f:
          for i, line in enumerate(f):
            try:
              result = convert_line_from_csv_gz(line, csv_gz_path)
              o.write(f"{json.dumps(result, separators=(',', ':'))}\n")
            except Exception as e:
              print(f"file: {csv_gz_path}", file=sys.stderr)
              print(f"line_index: {i}", file=sys.stderr)
              print(f"line: {line}", file=sys.stderr)

if __name__ == "__main__":

    PAIR = sys.argv[1]
    DATE = sys.argv[2]
    DATA_DIR = sys.argv[3]
    OUTPUT_DIR=sys.argv[4]

    start = int(time.time())

    output_path = f"{OUTPUT_DIR}/"
    file_paths = glob.glob(f"{DATA_DIR}/*/*")

    process_pair_day(output_path, file_paths)

    end = int(time.time())

    with open(f"{output_path}/_SUCCESS", "w") as f:
        f.write(f"time: {end-start}\n")
