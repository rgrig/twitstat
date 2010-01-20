# TODO: update index

from calendar import timegm
from contextlib import closing
from sys import stderr, stdin, stdout
from time import sleep, strptime
from urllib2 import urlopen
import shelve
import simplejson as json

def log(m):
  m = m.encode('utf-8')
  m = m.replace('\n', '  ')
  with open('statuses/log', 'a') as l:
    l.write(m)
    l.write('\n')
  if len(m) > 75:
    m = m[:72] + '...'
  stdout.write(m)
  stdout.write('\n')

def savePage(db, idx, results):
  global size
  lid = 0
  for r in results:
    try:
      user = r['from_user']
      text = r['text']
      id = int(r['id'])
      if str(id) in db:
        continue
      if lid < id:
        lid = id
      time = timegm(strptime(r['created_at'], '%a, %d %b %Y %H:%M:%S +0000'))
      db[str(id)] = {'user' : user, 'time' : time, 'text' : text}
      log(user + ': ' + text)
      idx[str(size)] = str(id)
      size += 1
    except Exception as e:
      stderr.write('ERROR: ' + str(e))
  return lid

urlBase = 'http://search.twitter.com/search.json?geocode=44.447924,26.097879,15.0km&rpp=100'

with open('statuses/indexsize', 'r') as f:
  size = int(f.readline())

with closing(shelve.open('statuses/data', 'c')) as db:
  with closing(shelve.open('statuses/index', 'c')) as idx:
    lid = 0
    try:
      with open('statuses/lastid', 'r') as idFile:
        lid = int(idFile.readline().strip())
# since_id is broken on twitter's side
#      urlBase = urlBase + '&since_id=' + str(lid)
    except:
      pass
    url = urlBase + '&page=1'
    page = json.load(urlopen(url))
    maxId = page['max_id']
    page = page['results']
    lid = max(lid, savePage(db, idx, page))
    if len(page) == 100:
      for i in range(2, 16):
        log('Waiting 25 seconds.')
        sleep(25)
        url = urlBase + '&page=' + str(i) + '&max_id=' + str(maxId)
        page = json.load(urlopen(url))['results']
        lid = max(lid, savePage(db, idx, page))
        if len(page) < 100:
          break

with open('statuses/lastid', 'w') as lidf:
  lidf.write(str(lid) + '\n')
with open('statuses/indexsize', 'w') as f:
  f.write(str(size) + '\n')

