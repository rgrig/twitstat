#!/usr/bin/env python2

# Creates the statuses/index file. It maps 0, 1, 2, ... to
# increasing status ids, which are keys in statuses/data.

from contextlib import closing
import shelve

with closing(shelve.open('statuses/data', 'c')) as db:
  with closing(shelve.open('statuses/index', 'c')) as idx:
    i = 0
    statuses = [int(x) for x in db.keys()]
    statuses.sort()
    for s in statuses:
      idx[str(i)] = str(s)
      i += 1
with open('statuses/indexsize', 'w') as f:
  f.write(str(i) + '\n')
