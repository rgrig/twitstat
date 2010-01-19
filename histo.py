# print the frequency of words appearing in statuses/data

from contextlib import closing
from sys import stdout
import shelve
import string

def ok(w):
  return len(w) >= 2

def lettersplit(s):
  r = []
  w = ''
  for c in s:
    if c not in string.letters:
      if w != '':
        r.append(w)
        w = ''
    else:
      w += c
  if w != '':
    r.append(w)
  return r

histo = dict()

with closing(shelve.open('statuses/data')) as db:
  for k in db.keys():
    for w in lettersplit(db[k]['text']):
      w = w.lower()
      if not ok(w):
        continue
      if w not in histo:
        histo[w] = 1
      else:
        histo[w] = histo[w] + 1

lst = [(v, k) for k, v in histo.items()]
lst.sort()
lst.reverse()
for k, v in lst:
  stdout.write('{0}\t{1}\n'.format(k, v.encode('utf-8')))
