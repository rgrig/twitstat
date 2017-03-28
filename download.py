#!/usr/bin/env python

# Creates the statuses/index file. It maps 0, 1, 2, ... to
# increasing status ids, which are keys in statuses/data.

from calendar import timegm
from contextlib import closing
import shelve
import simplejson as json
from sys import argv
from time import sleep, strptime
from urllib2 import urlopen


URL_BASE = 'http://search.twitter.com/search.json'

# TODO(rgrig): Get rid of code duplication with update_statuses.save_page
def download_specials():
  if len(argv) < 2:
    return
  url = URL_BASE + '?q=' + '+OR+'.join(['from:' + x for x in argv[1:]])
  url += '&rpp=100'
  with closing(shelve.open('statuses/data')) as db:
    for i in xrange(1,16):
      sleep(25)
      print 'getting page', i
      page = json.load(urlopen(url + '&page=' + str(i)))
      if i == 1:
        url += '&max_id=' + str(page['max_id'])
      for r in page['results']:
        user = r['from_user']
        text = r['text']
        id = int(r['id'])
        time = timegm(strptime(r['created_at'], '%a, %d %b %Y %H:%M:%S +0000'))
        #if str(id) in db: return
        db[str(id)] = {'user' : user, 'time' : time, 'text' : text}

def reindex():
  with closing(shelve.open('statuses/data')) as db:
    with closing(shelve.open('statuses/index')) as idx:
      i = 0
      statuses = [int(x) for x in db.keys()]
      statuses.sort()
      for s in statuses:
        idx[str(i)] = str(s)
        i += 1
  with open('statuses/indexsize', 'w') as f:
    f.write(str(i) + '\n')

def main():
  download_specials()
  reindex()

if __name__ == '__main__':
  main()
