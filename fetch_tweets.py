#!/usr/bin/env python3
# vim: set fileencoding=utf-8 :

from argparse import ArgumentParser
from calendar import timegm
from contextlib import closing
from time import sleep, strptime
from urllib.parse import quote

import db
import requests
import shelve
import sys

argparser = ArgumentParser(description='''
  Fetch tweets that match a query, and add them to db/tweets.
''')
argparser.add_argument('-q',
  help='query')
argparser.add_argument('-geocode', default='44.447924,26.097879,150km',
  help='e.g., 44.447924,26.097879,150km') # default = bucharest
argparser.add_argument('-count', default=100, type=int,
  help='batch size')
argparser.add_argument('-total', type=int,
  help='total number of tweets to fetch')
argparser.add_argument('-delay', default=3, type=int,
  help='delay between batches, in seconds')
argparser.add_argument('-verbose', action='store_true')

SEARCH_API_URL = 'https://api.twitter.com/1.1/search/tweets.json'
verbose = None

class Done(BaseException):  # used (rarely) for flow control
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
  if verbose and 'x-rate-limit-remaining' in r.headers:
    sys.stderr.write('api-rate-limit-remaining {}\n'.format(r.headers['x-rate-limit-remaining']))
  return r.json()


def build_query(q, geocode, count):
  assert count is not None
  query = '?count={}'.format(count)
  if q:
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
  query = build_query(args.q, args.geocode, args.count)
  processed = 0
  try:
    with shelve.open('db/tweets') as tweets:
      with shelve.open('db/raw') as raw:
        page = get('{}{}'.format(SEARCH_API_URL, query))
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
          sleep(args.delay)
          query = page['search_metadata']['next_results']
          page = get('{}{}'.format(SEARCH_API_URL, query))
  except Done:
    sys.stderr.write('fetched {} tweets (DONE)\n'.format(processed))
    postprocess_raw_tweets()

if __name__ == '__main__':
  main()

