# Extracts all statuses from a certain time range.
# It uses both statuses/data and statuses/index, so you may
# need to reindex before extracting information.

from calendar import timegm
from contextlib import closing
from sys import argv, exit, stdout
from time import strptime
import shelve

def parse_arg(s):
  return timegm(strptime(s, '%d %b %Y'))

if len(argv) != 3:
  print 'example: extract.py "15 Jan 2010" "16 Jan 2010"'
  exit(1)
start_time = parse_arg(argv[1])
stop_time = parse_arg(argv[2])
if stop_time <= start_time:
  print 'Second date should be strictly bigger.'
  exit(2)

with open('statuses/indexsize', 'r') as f:
  size = int(f.readline())

with closing(shelve.open('statuses/data', 'c')) as db:
  with closing(shelve.open('statuses/index', 'c')) as idx:
    low, high = 0, size
    while low + 1 != high:
      middle = (low + high) / 2
      status_time = db[idx[str(middle)]]['time']
      if status_time < start_time:
        low = middle
      else:
        high = middle
    while low < size:
      status = db[idx[str(low)]]
      if status['time'] >= stop_time:
        break
      stdout.write(status['text'].encode('utf-8').replace('\n', '  '))
      stdout.write('\n')
      low += 1

