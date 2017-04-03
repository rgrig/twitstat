#!/usr/bin/env python3

from argparse import ArgumentParser
from concurrent.futures import ProcessPoolExecutor
from util import phase

import requests
import shelve
import sys

argparser = ArgumentParser(description='''
  Changes db/slice to use normalized urls.

  The urls are normalized by following redirects. The result is cached in
  db/urls, to speed up future calls.
''')

argparser.add_argument('-n', '--nproc', default=100, type=int,
  help='how many processes/http-requests to run in parallel')
argparser.add_argument('-t', '--timeout', default=10, type=float,
  help='timeout for each url request')

def get_all_urls():
  urls = set()
  with shelve.open('db/slice') as tweets:
    for t in tweets.values():
      urls.update(t.mention.urls)
  phase('todo {} urls'.format(len(urls)))
  return urls

normalize_timeout = None
def normalize_one(u):
  global normalize_timeout
  try:
    un = requests.head(u, allow_redirects=True, timeout=normalize_timeout).url
  except Exception as e:
    print(e)
    un = None
  return (u, un)

def normalize_all(urls, nproc, timeout):
  global normalize_timeout
  normalize_timeout = timeout
  norm = {}
  todo = []
  with shelve.open('db/urls') as cache:
    for u in urls:
      if u in cache:
        norm[u] = cache[u]
      else:
        todo.append(u)
  phase('todo {} urls online'.format(len(todo)))
  with ProcessPoolExecutor(max_workers=nproc) as executor:
    for u, un in executor.map(normalize_one, todo, chunksize=10):
      if un is not None:
        norm[u] = un
  phase('finished http requests')

  with shelve.open('db/slice') as tweets:
    for i in list(tweets.keys()):
      t = tweets[i]
      new_urls = set()
      for u in t.mention.urls:
        if u in norm:
          new_urls.add(norm[u])
        else:
          new_urls.add(u)
      t.mention.urls = new_urls
      tweets[i] = t
  phase('updated db/slice')

  return norm

def save(norm):
  with shelve.open('db/urls') as cache:
    for k, v in norm.items():
      cache[k] = v
  phase('updated cache db/urls')

def main():
  args = argparser.parse_args()
  urls = get_all_urls()
  norm = normalize_all(urls, args.nproc, args.timeout)
  save(norm)

if __name__ == '__main__':
  main()
