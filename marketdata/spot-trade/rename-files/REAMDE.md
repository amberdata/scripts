# Renaming Files

# S3

```bash
cd /data3/shar
aws s3 cp s3://amberdata-marketdata/trade/ . --recursive --dryrun > s3.out 2> s3.err # 13.5 hours
time sed -e 's/.* \(s3.*\) to .*/\1/' s3.out > s3.out.files # ~37min
grep "@[0-9]*:[0-9]*:[0-9]*" s3.out.files > s3.out.files.unprocessed # ~10min
split -n 5000000 s3.out.files.unprocessed
mkdir unprocessed-parts
mv x* unprocessed-parts
```

For each 5 million line file I ran a screen session with 5 threads:
```bash
python3 rename-files.py -f unprocessed_parts/xab -t 5
```

**rename-files.py**

```python
import threading
import boto3
from argparse import ArgumentParser


####################################################################################                                                                                                                        

class RenamerThread(threading.Thread):

  def __init__(self, name, files, start, end):
    threading.Thread.__init__(self)
    self._name = name
    self._client = boto3.client('s3')
    self._files = files
    self._start = start
    self._end = end

  def run(self):
    print(f"{self._name}\tStart\tstart:\t{self._start}\tend:\t{self._end}")

    unprocessed = 0

    for i in range(self._start, self._end):

      file_path = self._files[i]
      #print(f"{self._name}\t\tfile_path: {file_path}")                                                                                                                                                     
      file_name = file_path.split('/')[-1]

      if ':' not in file_name:
        unprocessed += 1
        continue

      bucket = file_path.split('/')[2]
      key = '/'.join(file_path.split('/')[3:]).strip()
      new_key = key.replace(':', '-')
      try:
        copy_source = {'Bucket': bucket, 'Key': key}
                                                                                                                                                                    
        self._client.copy_object(CopySource=copy_source, Bucket=bucket, Key=new_key)
        self._client.delete_object(Bucket=bucket, Key=key)
      except Exception as e:
        #print(e)                                                                                                                                                                                           
        unprocessed += 1
        pass

      if self._start and i % 10000 == 0:
        print(f"{self._name}: {i}")
        #print(f"{self._name}\tfile:\t{i - self._start}\tof\t{self._end - self._start}")                                                                                                                    

      #print(f"{self._name} Exiting\t\tprocessed:\t{end - start - unprocessed}\tunprocessed:\t{unprocessed}") 

####################################################################################                                                                                                                        

parser = ArgumentParser()

parser.add_argument("-f", "--file", type=str, default=None, help="file containing list of files to change : to -")
parser.add_argument("-t", "--threads", type=int, default=1, help="number of threads")
parser.add_argument("-d", "--dryrun", type=bool, default=True, help="dry run mode")

args = parser.parse_args()

flp = args.file

if not flp:
  print(f"Must specify a file")
  quit()

files = []

with open(flp, 'r') as f:
  files = f.readlines()

num_lines = len(files)

def ranges(N, nb):
    step = N / nb
    return [(round(step*i), round(step*(i+1))) for i in range(nb)]

num_threads = args.threads

threads = []

for i, r in enumerate(ranges(num_lines, num_threads)):
  start, end = r
  threads.append(RenamerThread(f"Thread-{i}", files, start, end))

for t in threads:
  t.start()

for t in threads:
  t.join()

print("Exiting Main Thread")
```

# Database

- database: `ethereum-mainnet`
- table: `trade_s3_objects`
- column: `objectKey`

Sample:

```
 exchange |  pair   |    exchangeHour     |                        objectKey                        | size  |   timestampFirst    |    timestampLast    |     insertionTimestamp     |  tradeIdFirst  |  tradeIdLast   | objectKey2 | originalKeyExist 
----------+---------+---------------------+---------------------------------------------------------+-------+---------------------+---------------------+----------------------------+----------------+----------------+------------+------------------
 gdax     | bch_btc | 2019-06-06 23:00:00 | trade/bch_btc/2019-06-06/23/gdax-2019-06-07@00:25:00    | 16655 | 2019-06-06 23:23:46 | 2019-06-06 23:56:49 | 2019-06-07 00:28:04.243495 |        1652828 |        1652949 |            | 
 gdax     | bch_btc | 2019-06-07 00:00:00 | trade/bch_btc/2019-06-07/00/gdax-2019-06-07@00:25:00    | 10069 | 2019-06-07 00:00:02 | 2019-06-07 00:21:06 | 2019-06-07 00:28:04.305995 |        1652959 |        1653023 |            | 
 huobi    | srn_eth | 2019-06-06 23:00:00 | trade/srn_eth/2019-06-06/23/huobi-2019-06-07@00:25:00   | 19656 | 2019-06-06 23:23:07 | 2019-06-06 23:59:52 | 2019-06-07 00:28:04.4169   | 25607923999744 | 25607925315072 |            | 
 huobi    | srn_eth | 2019-06-07 00:00:00 | trade/srn_eth/2019-06-07/00/huobi-2019-06-07@00:25:00   | 12806 | 2019-06-07 00:00:04 | 2019-06-07 00:22:52 | 2019-06-07 00:28:04.466938 | 25607925319424 | 25607926342912 |            | 
 bithumb  | gto_krw | 2019-06-07 08:00:00 | trade/gto_krw/2019-06-07/08/bithumb-2019-06-07@00:25:00 | 85895 | 2019-06-07 08:25:17 | 2019-06-07 08:59:41 | 2019-06-07 00:28:04.593561 |       35695334 |       35695939 |            |
```

Sample use of `regexp_replace`:

```sql
UPDATE trade_s3_objects
SET "objectKey" = regexp_replace("objectKey", '([0-9]{2}):([0-9]{2}):([0-9]{2})', '\1-\2-\3')
WHERE size = 5;
```

Wrote scripts `run.sh` and `rename_day.sh`. To run the rename job:

```bash
./run.sh
```


