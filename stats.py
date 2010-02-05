#!/usr/bin/env python2
# vim: set fileencoding=utf-8 :

from calendar import timegm
from contextlib import closing
from multiprocessing import cpu_count, Pool
import re
import shelve
from sys import argv, exit, stderr, stdout
from time import localtime, mktime, strftime, struct_time, time
from urllib2 import urlopen

#{{{ usage
USAGE = """usage: ./stats.py [start_time [stop_time]]

The  default  start_time  is  the start  of  today;  the  default
stop_time is 24 hours after start_time. The time "20100120" means
"the beginning  of 20 Jan  2010" and the time  "2010012012" means
"20 Jan  2010 at midday".  In general the  format is a  string of
digits yyyymmddhhMMss. If some digits at the end are missing they
default to 0. All times are local.

The program creates three files.
  transcript.txt
    contains statuses in the specified range, one per line in the
    format 'AUTHOR: STATUS'
  words.txt
    contains the most used words, one per line in the format 'N W
    (AUTHORS) (FORMS)', where
      N is the number of AUTHORS that mentioned word W
      and FORMS are various forms of the word that were used
  urls.txt
    contains the most  mentioned http links, one per  line in the
    format 'N URL (AUTHORS) (FORMS)', where
      N is the number of AUTHORS that mentioned URL

The program expects a database of Twitter statuses in ./statuses.
"""
#}}}

#{{{ small utils
proftime = time()
def here(s):
  global proftime
  nt = time()
  print s, nt - proftime
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
ROMSIMPL = dict([(ROMNICE[i], ROMUGLY[i]) for i in xrange(len(ROMNICE))])
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
URL_REGEX = 'http://' + hostport + opt('/' + hpath + opt(r'\?' + hsegment))
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
  if isinstance(wn, unicode):
    wn = wn.encode('utf-8')
  return (w, wn)

def normalize_url(u):
  try:
    un = urlopen(u, timeout=10).geturl()
  except Exception as e:
    un = u
  if isinstance(un, unicode):
    un = un.encode('utf-8')
  return (u, un)

def normalize_all_words(l):
  r = dict()
  processes = Pool(max(1, cpu_count()-1))
  for _, s in l:
    for w, wn in processes.imap_unordered(normalize_word, s):
      r[w] = wn
  return r

def normalize_all_urls(l):
  r = dict()
  with closing(shelve.open('statuses/urls', 'c')) as cache:
    all_urls = set()
    for _, s in l:
      for u in s:
        all_urls.add(u)
    print '  ', len(all_urls), 'urls to normalize'
    unknown_urls = []
    for u in all_urls:
      try:
        r[u] = cache[u.encode('utf-8')]
      except KeyError:
        unknown_urls.append(u)
    print '  ', len(unknown_urls), 'urls to normalize online'
    processes = Pool(25)
    for u, un in processes.imap_unordered(normalize_url, unknown_urls, 10):
      r[u] = un
      cache[u.encode('utf-8')] = un
  return r

def dummy_normalize(l):
  r = dict()
  for _, s in l:
    for m in s:
      r[m] = m
  return r
#}}}

statuses_of_user = dict()

#{{{ extraction of features from statuses
def match_and_bin(regex, normalize):
  '''Find all the matches for regex. For each normalized match it
  computes who said it. For each normalized match it remembers all
  the non-normalized forms.'''

  # construct a list of sets of matches
  pattern = re.compile(regex)
  matches = []
  for user, statuses in statuses_of_user.iteritems():
    user_matches = set()
    for s in statuses:
      for m in re.finditer(pattern, s):
        user_matches.add(m.group())
    matches.append((user, user_matches))
  normalized = normalize(matches)

  # for each normalized match keep the set of users
  # also, compute forms, the inverse of normalized
  users = dict()
  forms = dict()
  for user, user_matches in matches:
    for m in user_matches:
      mn = normalized[m]
      if mn in STOPWORDS:
        continue
      if mn not in forms:
        forms[mn] = set()
        users[mn] = set()
      forms[mn].add(m)
      users[mn].add(user)
  return (users, forms)

def dump_histogram(users, forms, filename):
  '''sort by the number of users mentioning each match
  and report to file'''
  with open(filename, 'w') as file:
    list = [(len(us), m) for m, us in users.iteritems()]
    list.sort()
    list.reverse()
    for n, m in list:
      file.write(str(n))
      file.write(' ')
      file.write(m)
      file.write(' (')
      for author in users[m]:
        file.write(' ')
        file.write(author.encode('utf-8'))
      file.write(' ) (')
      for form in forms[m]:
        file.write(' ')
        file.write(form.encode('utf-8'))
      file.write(' )\n')

def compute_histogram(regex, normalize, filename):
  users, forms = match_and_bin(regex, normalize)
  dump_histogram(users, forms, filename)
#}}}

#{{{ graph related operations
def compute_talkgraph(users):
  mentions = dict()
  for w, us in users.iteritems():
    if w.startswith('@'):
      _, wn = normalize_word(w[1:])
      for u in us:
        _, un = normalize_word(u)
        if un not in mentions:
          mentions[un] = set()
        mentions[un].add(wn)
  with open('talkgraph.txt', 'w') as file:
    for x, ys in mentions.iteritems():
      file.write(x)
      file.write(' ->')
      for y in ys:
        file.write(' ')
        file.write(y)
      file.write('\n')
#}}}

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
  with closing(shelve.open('statuses/data', 'c')) as db:
    with closing(shelve.open('statuses/index', 'c')) as idx:
      low, high = -1, size
      while low + 1 < high:
        binstep += 1
        middle = (low + high) / 2
        status_time = db[idx[str(middle)]]['time']
        if status_time < start_time:
          low = middle
        else:
          high = middle
      print '  binstep',binstep
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
          transcript.write(id + ' ')
          transcript.write(status['user'].encode('utf-8') + ': ')
          transcript.write(status['text'].encode('utf-8').replace('\n', '  '))
          transcript.write('\n')
          low += 1

def main():
  parse_command_line()
  here('initializare')
  extract_and_bin()
  here('extracted and binned')

  # compute histograms for words and for urls
  users, forms = match_and_bin(WORD_REGEX, normalize_all_words)
  dump_histogram(users, forms, 'words.txt')
  here('words.txt')
  compute_talkgraph(users)
  here('talkgraph.txt')
  compute_histogram(URL_REGEX, normalize_all_urls, 'urls.txt')
  here('urls.txt')

if __name__ == '__main__':
  main()
