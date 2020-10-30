#!/usr/bin/env python3

from argparse import ArgumentParser
from collections import defaultdict

import shelve
import sys

argparser = ArgumentParser(description='''
  Creates db/urlrank, by distributing user scores (db/userrank)
  to the urls they mentioned (in db/slice).
''')

argparser.add_argument('-n', '--toprint', default=10, type=int,
  help='how many urls to report to stdout')
argparser.add_argument('-e', '--endorsers', action='store_true',
  help='report which twitter authors mentioned the url')
argparser.add_argument('-f', '--filter', default='twitter.com',
  help='do not include urls containing a certain substring')

def main():
  args = argparser.parse_args()
  urls_of_user = defaultdict(list)
  with shelve.open('db/slice') as tweets:
    for t in tweets.values():
      for u in t.mention.urls:
        if u.find(args.filter) == -1:
          urls_of_user[t.author].append(u)
  endorsers_of_url = defaultdict(set)
  if args.endorsers:
    with shelve.open('db/users') as users:
      for u, ls in urls_of_user.items():
        for l in ls:
          endorsers_of_url[l].add(users[u].screen_name)
  if False:
    url_counts = defaultdict(int)
    for ls in urls_of_user.values():
      url_counts[len(ls)] += 1
    for sz, cnt in sorted(url_counts.items()):
      sys.stderr.write('freq {} {}\n'.format(sz, cnt))
  if False:
    user_counts = defaultdict(int)
    for u, ls in urls_of_user.items():
      for l in ls:
        user_counts[l] += 1
    for cnt, u in sorted((-cnt, u) for u, cnt in user_counts.items()):
      sys.stderr.write('freq {} {}\n'.format(-cnt, u))
  score_of_url = defaultdict(float)
  with shelve.open('db/userrank') as userrank:
    def us(uid):
      return userrank[uid] if uid in userrank else 0
    for u, urls in urls_of_user.items():
      if not urls:
        continue
      s = us(u) / len(urls)
      for l in urls:
        #sys.stderr.write('{:.2f} from {} to {}\n'.format(s,u,l))
        score_of_url[l] += s
  with shelve.open('db/urlrank', 'n') as urlrank:
    for l, s in score_of_url.items():
      urlrank[l] = s
  sys.stderr.write('ranked {} urls\n'.format(len(score_of_url)))
  xs = sorted((-s, l) for l, s in score_of_url.items())
  for s, l in xs[:args.toprint]:
    sys.stdout.write('{:9.6f} {}'.format(-s, l))
    for u in sorted(endorsers_of_url[l]):
      sys.stdout.write(' {}'.format(u))
    sys.stdout.write('\n')

if __name__ == '__main__':
  main()
