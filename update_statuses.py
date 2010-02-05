#!/usr/bin/env python2
# vim: set fileencoding=utf-8 :

from calendar import timegm
from contextlib import closing
from sys import stderr, stdin, stdout
from time import sleep, strptime
from urllib2 import urlopen
import shelve
import simplejson as json

size = 0  # number of statuses in the database

class NoNewResults:  # used (rarely) for flow control
  pass

def log(m):
  m = m.replace('\n', '  ')
  me = m.encode('utf-8')
  with open('statuses/log', 'a') as l:
    l.write(me)
    l.write('\n')
  if len(m) > 75:
    m = m[:72] + '...'
  stdout.write(m.encode('utf-8'))
  stdout.write('\n')

def save_page(db, idx, results):
  global size
  for r in results:
    user = r['from_user']
    text = r['text']
    id = int(r['id'])
    if str(id) in db:
      raise NoNewResults()
    time = timegm(strptime(r['created_at'], '%a, %d %b %Y %H:%M:%S +0000'))
    db[str(id)] = {'user' : user, 'time' : time, 'text' : text}
    log(user + ': ' + text)
    idx[str(size)] = str(id)
    size += 1

GEOCODE = '44.447924,26.097879,15.0km'  # Bucharest

def main():
  global size
  url_base = 'http://search.twitter.com/search.json'
  url_base += '?geocode=' + GEOCODE + '&rpp=100'
  with open('statuses/indexsize', 'r') as f:
    size = int(f.readline())
  with closing(shelve.open('statuses/data', 'c')) as db:
    with closing(shelve.open('statuses/index', 'c')) as idx:
      try:
        url = url_base + '&page=1'
        page = json.load(urlopen(url))
        url_base += '&max_id=' + str(page['max_id'])
        save_page(db, idx, page['results'])
        for i in range(2, 16):
          log('Waiting 25 seconds.')
          sleep(25)
          url = url_base + '&page=' + str(i)
          save_page(db, idx, json.load(urlopen(url))['results'])
      except NoNewResults:
        pass # normal ending
  with open('statuses/indexsize', 'w') as f:
    f.write(str(size) + '\n')


if __name__ == '__main__':
  main()

