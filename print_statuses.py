# Print the content of statuses/data

from contextlib import closing
from sys import stdout
import shelve
import time

with closing(shelve.open('statuses/data')) as db:
  keys = db.keys()
  keys.sort()
  for k in keys:
    v = db[k]
    stdout.write('{0}@{2} {1}: {3}\n'.format(k, v['user'].encode('utf-8'), v['time'], v['text'].encode('utf-8')))
  stdout.write('SIZE: {0} statuses\n'.format(len(keys)))
