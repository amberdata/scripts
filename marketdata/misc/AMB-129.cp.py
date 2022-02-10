import sys
import glob

BUCKET = "amberdata-shar"

def parse(suffix):
    parts = suffix.split("/")
    pair = parts[0].split("=")[1]
    dt = parts[1].split("=")[1]
    hr = parts[2].split("=")[1]
    exchange = parts[3].split("=")[1]
    return (pair, dt, hr, exchange)

def print_commands(OUTPUT_DIR, file_paths):
    for file_path in file_paths:
        suffix = file_path.replace(f"{OUTPUT_DIR}/", "")
        (pair, dt, hr, exchange) = parse(suffix)
        output_s3_file = f"s3://{BUCKET}/trade/{pair}/{dt}/{hr}/{exchange}-2022-02-10@00-00-00"
        cmd = f"aws s3 cp {file_path} {output_s3_file}"
        print(cmd)

if __name__ == "__main__":

    DATA_DIR = sys.argv[1]

    start = int(time.time())

    # sample file, DATA_DIR=/data4/shar/AMB-129/
    # /data4/shar/AMB-129/_pair=sol_usd/_date=2021-08-22/_hour=16/_exchange=kraken/part-00930-3273d399-96a3-457b-aed8-49a9ff5935de.c000.json
    file_paths = glob.glob(f"{DATA_DIR}/*/*/*/*/*")

    print_commands(DATA_DIR, file_paths)

    end = int(time.time())