#!/usr/bin/env python3
# vim: set fileencoding=utf-8 :

from calendar import timegm
from contextlib import closing
from sys import stderr, stdin, stdout
from time import sleep, strptime

import requests
import shelve

size = 0  # number of statuses in the database

class NoNewResults(BaseException):  # used (rarely) for flow control
  pass

oauth2_headers = None

def get(url):
  global oauth2_headers
  if oauth2_headers is None:
    with open('secret.credentials') as f:
      for line in f:
        ws = line.split()
        if ws[0] == 'access-token':
          oauth2_headers = {'Authorization' : 'Bearer {}'.format(ws[1])}
          break
  if oauth2_headers is None:
    sys.stderr.write('Please run oauth.py\n')
    raise NoNewResults
  r = requests.get(url, headers=oauth2_headers)
  return r.json()

def log(m):
  m = m.replace('\n', '  ')
  with open('statuses/log', 'a') as l:
    l.write('{}\n'.format(m))
  if len(m) > 75:
    m = m[:72] + '...'
  stdout.write('{}\n'.format(m))

def save_page(db, idx, results):
  global size
  for r in results:
    user = r['user']['name']
    text = r['text']
    id = int(r['id'])
    if str(id) in db:
      raise NoNewResults()
    time = timegm(strptime(r['created_at'], '%a %b %d %H:%M:%S +0000 %Y'))
    db[str(id)] = {'user' : user, 'time' : time, 'text' : text}
    log(user + ': ' + text)
    idx[str(size)] = str(id)
    size += 1

GEOCODE = '44.447924,26.097879,15.0km'  # Bucharest

def main():
  global size
  url_base = 'https://api.twitter.com/1.1/search/tweets.json'
  with open('statuses/indexsize', 'r') as f:
    size = int(f.readline())
  with closing(shelve.open('statuses/data')) as db:
    with closing(shelve.open('statuses/index')) as idx:
      try:
        url = '{}?geocode={}&count=100'.format(url_base, GEOCODE)
        page = get(url)
        save_page(db, idx, page['statuses'])
        for i in range(2, 64):
          if 'next_results' not in page['search_metadata']:
            break
          log('Waiting 25 seconds.')
          sleep(25)
          url = '{}{}'.format(url_base, page['search_metadata']['next_results'])
          page = get(url)
          save_page(db, idx, page['statuses'])
      except NoNewResults:
        pass # normal ending
  with open('statuses/indexsize', 'w') as f:
    f.write(str(size) + '\n')


if __name__ == '__main__':
  main()

