#!/usr/bin/env python3
# vim: set fileencoding=utf-8 :

from calendar import timegm
from contextlib import closing
from multiprocessing import cpu_count, Pool
import re
import shelve
from sys import argv, exit, stderr, stdout
from time import localtime, mktime, strftime, struct_time, time

import requests

#{{{ usage
USAGE = """usage: ./stats.py [start_time [stop_time]]

The  default  start_time  is  the start  of  today;  the  default
stop_time is 24 hours after start_time. The time "20100120" means
"the beginning  of 20 Jan  2010" and the time  "2010012012" means
"20 Jan  2010 at midday".  In general the  format is a  string of
digits yyyymmddhhMMss. If some digits at the end are missing they
default to 0. All times are local.

The program creates two files.
  transcript.txt
    contains statuses in the specified range, one per line in the
    format 'AUTHOR: STATUS'
  histograms
    contains words and urls, with counts

The program expects a database of Twitter statuses in ./statuses.
"""
#}}}

#{{{ small utils
proftime = time()
def here(s):
  global proftime
  nt = time()
  print(s, nt - proftime)
  proftime = nt

# These are utilities for building regular expressions.
def opt(s): 
  return '(' + s + ')?'
def lst(s): 
  return '(' + s + ')*'
def alt(l): 
  return '(' + '|'.join(['('+x+')' for x in l]) + ')'

ROMNICE = u'ăîÎșşȘțţȚŢâÂăĂ'
ROMUGLY = u'aiIssSttTTaAaA'
ROMSIMPL = dict([(ROMNICE[i], ROMUGLY[i]) for i in range(len(ROMNICE))])
WORD_REGEX = u'[@#]?[a-zA-Z0-9' + ROMNICE + u'_-]{3,}'
# see RFC1738
hex = '[0-9a-fA-F]'
escape = '%' + hex + hex
unreserved = r'[a-zA-Z0-9\$_\.\+!\*\'\(\),-]'
uchar = alt([unreserved, escape])
hsegment = lst(alt([uchar, '[;:@&=]']))
hpath = hsegment + lst('/' + hsegment)
hostnumber = r'[0-9]\.[0-9]\.[0-9]\.[0-9]'
toplabel = '[a-zA-Z]' + opt(lst('[a-zA-Z0-9]|-') + '[a-zA-Z0-9]')
domainlabel = '[a-zA-Z0-9]' + opt(lst('[a-zA-Z0-9]|-') + '[a-zA-Z0-9]')
hostname = lst(domainlabel + r'\.') + toplabel
host = alt([hostname, hostnumber])
hostport = host + opt(':[0-9]+')
URL_REGEX = 'http' + opt('s') + '://' + hostport + opt('/' + hpath + opt(r'\?' + hsegment))

# hack
#URL_REGEX = 'https://t.co/[0-9a-zA-Z]+'
with open('stopwords', 'r') as f:
  STOPWORDS = set([x.strip() for x in f.readlines()])

# globals set by the command line
start_time = 0
stop_time = 0

def parse_time(s):
  t = s + (14 - len(s)) * '0'
  try:
    return mktime(struct_time((
      int(t[0:4]),
      int(t[4:6]),
      int(t[6:8]),
      int(t[8:10]),
      int(t[10:12]),
      int(t[12:14]),
      0,0,-1)))
  except Exception as e:  # TODO: be more specific
    stderr.write(USAGE)
    stderr.write('I cannot parse the time {0}: {1}\n'.format(s, str(e)))
    exit(3)
#}}}

#{{{ normalize functions
def normalize_word(w):
  def romsimpl(c):
    if c not in ROMSIMPL:
      return c
    return ROMSIMPL[c]
  wn = ''.join([romsimpl(c) for c in w]) 
  wn = wn.lower()
  return (w, wn)

def normalize_url(u):
  try:
    un = urlopen(u, timeout=10).url
  except Exception as e:
    un = u
  return (u, un)

def normalize_all_words(l):
  r = dict()
  processes = Pool(max(1, cpu_count()-1))
  for s in l.values():
    for w, wn in processes.imap_unordered(normalize_word, s):
      r[w] = wn
  return r

def normalize_all_urls(l):
  r = dict()
  with closing(shelve.open('statuses/urls')) as cache:
    all_urls = set()
    for s in l.values():
      for u in s:
        all_urls.add(u)
    print('  ', len(all_urls), 'urls to normalize')
    unknown_urls = []
    for u in all_urls:
      try:
        r[u] = cache[u]
      except KeyError:
        unknown_urls.append(u)
    print('  ', len(unknown_urls), 'urls to normalize online')
    processes = Pool(25)
    for u, un in processes.imap_unordered(normalize_url, unknown_urls, 10):
      r[u] = un
      cache[u] = un
  return r

def dummy_normalize(l):
  '''For testing.'''
  r = dict()
  for s in l.values():
    for m in s:
      r[m] = m
  return r
#}}}

statuses_of_user = dict()

#{{{ extraction of features from statuses
def match_and_bin(regex, normalize):
  '''Computes a histogram of normalized matches per user.

  A 'match' is a substring in the language of regex. Two
  matches that normalize to the same thing are considered to be
  equivalent: They are different 'forms' of the same normalized
  match.

  This returns a dictionary that gives, for each user, a
  histogram of normalized matches. It also returns a dictionary
  that gives, for each normalized match, its forms.'''

  # for each user, list the matches, then normalize
  pattern = re.compile(regex)
  matches = dict()
  for user, statuses in statuses_of_user.items():
    user_matches = []
    for s in statuses:
      for m in re.finditer(pattern, s):
        user_matches.append(m.group())
    matches[user] = user_matches
  normalized = normalize(matches)

  # for each user, compute the histogram of normalized matches
  # also, for each normalized match, compute the set of forms
  histo_of_user = dict()
  forms = dict()
  for user, user_matches in matches.items():
    histo = dict()
    for m in user_matches:
      mn = normalized[m]
      if mn in STOPWORDS:
        continue
      if mn not in forms:
        forms[mn] = set()
      forms[mn].add(m)
      if mn not in histo:
        histo[mn] = 0
      histo[mn] += 1
    histo_of_user[user] = histo
  return (histo_of_user, forms)

def aggregate_histograms(histo_of_user, filter):
  histo = dict()
  for counts in histo_of_user.values():
    for match, count in counts.items():
      if match not in histo:
        histo[match] = 0
      histo[match] += filter(count)
  return histo

def dump_histogram(histo, forms, filename):
  '''print in decreasing order of frequency'''
  list = [(cnt, m) for m, cnt in histo.items()]
  list.sort(lambda x, y: cmp(y, x))
  with open(filename, 'w') as file:
    for n, m in list:
      file.write(str(n))
      file.write(' ')
      file.write(m)
      file.write(' (')
      for form in forms[m]:
        file.write(' {}'.format(form))
      file.write(' )\n')
#}}}

def save_histograms(words_of_user, urls_of_user):
  def get(d, u):
    if u in d:
      return d[u]
    else:
      return dict()
  all_users = set()
  all_users.update(words_of_user.keys())
  all_users.update(urls_of_user.keys())
  with closing(shelve.open('histograms', 'n')) as f:
    for u in all_users:
      urls = get(urls_of_user, u)
      old_words = get(words_of_user, u)
      mentions = dict()
      words = dict()
      for w, cnt in old_words.items():
        if w.startswith('@'):
          wn = w[1:]
          mentions[wn] = cnt
        else:
          words[w] = cnt
      _, un = normalize_word(u)
      data = {'urls' : dict(), 'words' : dict()}
      f[un] = {'words' : words, 'urls' : urls, 'mentions' : mentions}

#{{{ cmd line parsing
def parse_command_line():
  global start_time
  global stop_time
  if len(argv) == 1:
    start_time = parse_time(strftime('%Y%02m%02d', localtime()))
    stop_time = start_time + 60 * 60 * 24
  elif len(argv) == 2:
    start_time = parse_time(argv[1])
    stop_time = start_time + 60 * 60 * 24
  elif len(argv) == 3:
    start_time = parse_time(argv[1])
    stop_time = parse_time(argv[2])
  else:
    stderr.write(USAGE)
    exit(1)
  if start_time >= stop_time:
    stderr.write('Void time interval.\n')
    exit(2)
#}}}

def extract_and_bin():
  '''Go through the database and generate the file transcript.txt.
  At the same time make a list, for each user, with its statuses.'''
  global start_time
  global stop_time
  binstep = 0
  with open('statuses/indexsize', 'r') as f:
    size = int(f.readline())
  with closing(shelve.open('statuses/data')) as db:
    with closing(shelve.open('statuses/index')) as idx:
      low, high = -1, size
      while low + 1 < high:
        binstep += 1
        middle = (low + high) // 2
        status_time = db[idx[str(middle)]]['time']
        if status_time < start_time:
          low = middle
        else:
          high = middle
      print('  binstep',binstep)
      low += 1
      here('binsearch')
      with open('transcript.txt', 'w') as transcript:
        while low < size:
          id = idx[str(low)]
          status = db[id]
          if status['time'] >= stop_time:
            break
          if status['user'] not in statuses_of_user:
            statuses_of_user[status['user']] = []
          statuses_of_user[status['user']].append(status['text'])
          transcript.write('{} {}: {}\n'.format(
            id,
            status['user'],
            status['text'].replace('\n', '   ')))
          low += 1

def main():
  parse_command_line()
  extract_and_bin()
  here('read database')
  words_of_user, _ = match_and_bin(WORD_REGEX, normalize_all_words)
  here('got words')
  urls_of_user, _ = match_and_bin(URL_REGEX, normalize_all_urls)
  here('got urls')
  save_histograms(words_of_user, urls_of_user)
  here('saved histograms')

if __name__ == '__main__':
  main()
