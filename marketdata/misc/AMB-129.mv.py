import sys

BUCKET = 'amberdata-shar'
DIR = 'trade-old'

def parse(path):
    parts = path.split("/")
    pair = parts[4]
    dt = parts[5]
    hr = parts[6]
    file_name = parts[7]
    return (pair, dt, hr, file_name)

def print_commands(file_list_files):
    with open(file_list_files, 'r') as f:
        while True:
            line = f.readline()
            if not line:
                break
            line = line.strip()
            (pair, dt, hr, file_name) = parse(line)
            if not pair in ['ada_usd', 'btc_usd', 'eth_btc', 'eth_usd', 'eth_usdt', 'sol_usd']:
                break
            if dt < '2021-06-01' or dt > '2021-02-08':
                break

            cmd = f"aws s3 mv {line} s3://{BUCKET}/{DIR}/{pair}/{dt}/{hr}/{file_name}"

            print(cmd)
            
if __name__ == "__main__":

    FILE_LIST_FILE = sys.argv[1]

    print_commands(FILE_LIST_FILE)