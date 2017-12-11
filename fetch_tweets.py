#!/usr/bin/env python3
# vim: set fileencoding=utf-8 :

from argparse import ArgumentParser
from calendar import timegm
from pathlib import Path
from time import sleep, strptime, time
from urllib.parse import quote

import db
import json
import requests
import shelve
import sys

argparser = ArgumentParser(description='''
  Fetch tweets that match a query, and add them to db/tweets.
''')

defaultargs = {}
configpath = Path('db/config.json')
try:
  with configpath.open() as config:
    defaultargs = json.load(config)
    if False:
      print(defaultargs)
except Exception as e:
  sys.stderr.write('W: no db/config.json {}\n'.format(e))
def argdef(k):
  return defaultargs[k] if k in defaultargs else None


argparser.add_argument('-q',
  help='query')
argparser.add_argument('-geocode', default=argdef('geocode'),
  help='e.g., 44.447924,26.097879,150km')
argparser.add_argument('-authors', default=argdef('authors'), nargs='+',
  help='filter by author')
argparser.add_argument('-count', default=argdef('count'), type=int,
  help='batch size')
argparser.add_argument('-total', default=argdef('total'), type=int,
  help='total number of tweets to fetch')
argparser.add_argument('-delay', default=argdef('delay'), type=float,
  help='delay between batches, in seconds')
argparser.add_argument('-verbose', action='store_true')

SEARCH_API_URL = 'https://api.twitter.com/1.1/search/tweets.json'
verbose = None

class Done(BaseException):  # used (rarely) for flow control
  pass

oauth2_headers = None

last_get = None
def get(url, delay):
  global oauth2_headers
  global last_get
  if last_get:
    sleep(max(0, time() - delay - last_get))
  if False:
    print('GET ', url)
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
  if verbose and 'x-rate-limit-remaining' in r.headers:
    sys.stderr.write('api-rate-limit-remaining {}\n'.format(r.headers['x-rate-limit-remaining']))
  last_get = time()
  return r.json()


def build_query(q, geocode, count, authors):
  query = ''
  query += '?result_type=recent'
  assert count is not None
  query += '&count={}'.format(count)
  if authors:
    if q is None:
      q = ''
    qs = ['{} from:{}'.format(q, a) for a in authors]
    query += '&q={}'.format(quote(' OR '.join(qs)))
  elif q:
    query += '&q={}'.format(quote(q))
  if geocode:
    query += '&geocode={}'.format(geocode)
  return query


def time_of_raw_tweet(t):
  assert 'created_at' in t
  return timegm(strptime(t['created_at'], '%a %b %d %H:%M:%S +0000 %Y'))


def postprocess_raw_tweets():
  with shelve.open('db/raw') as raw:
    # Update users.
    with shelve.open('db/users') as users:
      for t in raw.values():
        u = t['user']
        users[u['id_str']] = db.User(u['screen_name'])
        for u in t['entities']['user_mentions']:
          users[u['id_str']] = db.User(u['screen_name'])
        if t['in_reply_to_user_id_str']:
          if t['in_reply_to_screen_name']:
            users[t['in_reply_to_user_id_str']] = db.User(t['in_reply_to_screen_name'])

    # Update tweets.
    with shelve.open('db/tweets') as tweets:
      ids = list(raw.keys())
      for i in ids:
        if i in tweets:
          sys.stderr.write('W: tweet {} already in db\n'.format(i))
        else:
          t = raw[i]
          text = t['text']
          time = time_of_raw_tweet(t)
          author = t['user']['id_str']
          mention = db.Mention()
          for u in t['entities']['user_mentions']:
            mention.users.add(u['id_str'])
          if t['in_reply_to_user_id_str']:
            mention.users.add(t['in_reply_to_user_id_str'])
          for u in t['entities']['urls']:
            mention.urls.add(u['expanded_url'])
          if 'retweeted_status' in t:
            mention.tweets.add(t['retweeted_status']['id_str'])
            mention.users.add(t['retweeted_status']['user']['id_str'])
          if 'quoted_status' in t:
            mention.tweets.add(t['quoted_status']['id_str'])
          tweets[i] = db.Tweet(text, time, author, mention)
        del raw[i]

bad_times = False
def check_times(tweets):
  global bad_times
  if bad_times:
    return
  p = None
  bad = False
  for t in tweets:
    if p is None:
      p = time_of_raw_tweet(t)
    else:
      n = time_of_raw_tweet(t)
      if p < n:
        bad_times = True
        sys.stderr.write('W: tweet times are not ordered\n')


def main():
  global verbose
  args = argparser.parse_args()
  verbose = args.verbose
  if not args.authors:
    args.authors = [[]]
  else:
    new_authors = []
    i = 0
    while i < len(args.authors):
      new_authors.append(args.authors[i:i+5])
      i += 5
    args.authors = new_authors
  args.total = 1 + args.total // len(args.authors)
  for authors in args.authors:
    processed = 0
    sys.stderr.write('fetching {} from {}\n'.format(args.total, ' '.join(authors)))
    query = build_query(args.q, args.geocode, args.count, authors)
    try:
      with shelve.open('db/tweets') as tweets:
        with shelve.open('db/raw') as raw:
          page = get('{}{}'.format(SEARCH_API_URL, query), args.delay)
          while True:
            check_times(page['statuses'])
            for s in page['statuses']:
              if s['id_str'] in tweets:
                raise Done # assumes that times are descending
              raw[s['id_str']] = s
              processed += 1
              if args.total and processed >= args.total:
                raise Done
            raw.sync()
            sys.stderr.write('fetched {} tweets\n'.format(processed))
            if 'next_results' not in page['search_metadata']:
              sys.stderr.write('W: gap in tweet data; run me more often\n')
              raise Done
            query = page['search_metadata']['next_results']
            page = get('{}{}'.format(SEARCH_API_URL, query), args.delay)
    except Done:
      sys.stderr.write('fetched {} tweets (DONE)\n'.format(processed))
    postprocess_raw_tweets()

if __name__ == '__main__':
  main()

