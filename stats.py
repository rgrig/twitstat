#!/usr/bin/env python2

from calendar import timegm
from contextlib import closing
import shelve
from sys import argv, exit, stderr, stdout
from time import localtime, mktime, strftime, struct_time

USAGE = """usage: ./stats.py [start_time [stop_time]]

The  default  start_time  is  the start  of  today;  the  default
stop_time is 24 hours after start_time. The time "20100120" means
"the beginning  of 20 Jan  2010" and the time  "2010012012" means
"20 Jan  2010 at midday".  In general the  format is a  string of
digits yyyymmddhhMMss. If some digits at the end are missing they
default to 0. All times are local.

The program creates three files.
  transcript.txt
    contains statuses in the specified range, one per line in the
    format 'AUTHOR: STATUS'
  words.txt
    contains the most used words, one per line in the format 'N W
    (AUTHORS) (FORMS)', where
      N is the number of AUTHORS that mentioned word W
      and FORMS are various forms of the word that were used
  urls.txt
    contains the most  mentioned http links, one per  line in the
    format 'N URL (AUTHORS)', where
      N is the number of AUTHORS that mentioned URL

The program expects a database of Twitter statuses in ./statuses.
"""

# globals set by the command line
start_time = 0
stop_time = 0

def parse_time(s):
  t = s + (14 - len(s)) * '0'
  try:
    return mktime(struct_time((
      int(t[0:4]),
      int(t[4:6]),
      int(t[6:8]),
      int(t[8:10]),
      int(t[10:12]),
      int(t[12:14]),
      0,0,-1)))
  except Exception as e:
    stderr.write(USAGE)
    stderr.write('I cannot parse the time {0}: {1}\n'.format(s, str(e)))
    exit(3)

### The main program starts here.

# Parse the command line.
if len(argv) == 1:
  start_time = parse_time(strftime('%Y%02m%02d', localtime()))
  stop_time = start_time + 60 * 60 * 24
elif len(argv) == 2:
  start_time = parse_time(argv[1])
  stop_time = start_time + 60 * 60 * 24
elif len(argv) == 3:
  start_time = parse_time(argv[1])
  stop_time = parse_time(argv[2])
else:
  stderr.write(USAGE)
  exit(1)
if start_time >= stop_time:
  stderr.write('Void time interval.\n')
  exit(2)

# Go through the database and generate the file transcript.txt.
# At the same time make a list, for each user, with its statuses.
with open('statuses/indexsize', 'r') as f:
  size = int(f.readline())
with closing(shelve.open('statuses/data', 'c')) as db:
  with closing(shelve.open('statuses/index', 'c')) as idx:
    low, high = -1, size
    while low + 1 < high:
      middle = (low + high) / 2
      status_time = db[idx[str(middle)]]['time']
      if status_time < start_time:
        low = middle
      else:
        high = middle
    low += 1
    with open('transcript.txt', 'w') as transcript:
      while low < size:
        id = idx[str(low)]
        status = db[id]
        if status['time'] >= stop_time:
          break
        transcript.write(id + ' ')
        transcript.write(status['user'].encode('utf-8') + ': ')
        transcript.write(status['text'].encode('utf-8').replace('\n', '  '))
        transcript.write('\n')
        low += 1

