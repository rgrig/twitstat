#!/usr/bin/env python3

from argparse import ArgumentParser
from time import localtime, mktime, strftime, struct_time

import shelve
import shutil

argparser = ArgumentParser(description='''
  Extracts a time range from db/tweets and stores it in db/slice.
''')

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
    raise ValueError(e)

def today():
  return parse_time(strftime('%Y%02m%02d', localtime()))

argparser.add_argument('-o', action='store_true',
  help='at the end, replace db/tweets by db/slice')
argparser.add_argument('starttime', nargs='?', type=parse_time,
  help='e.g., 201704021130, or 201704 = 201704010000')
argparser.add_argument('stoptime', nargs='?', type=parse_time,
  help='e.g., 201704021130, or 201704 = 201704010000')

def main():
  args = argparser.parse_args()
  if args.starttime is None:
    args.starttime = today()
  if args.stoptime is None:
    args.stoptime = args.starttime + 60 * 60 * 24
  with shelve.open('db/tweets') as tweets:
    with shelve.open('db/slice','n') as slice:
      for i, t in tweets.items():
        if not (args.starttime <= t.time < args.stoptime):
          continue
        slice[i] = t
  if args.o:
    shutil.move('db/tweets', 'db/tweets.bck')
    shutil.move('db/slice', 'db/tweets')

if __name__ == '__main__':
  main()
