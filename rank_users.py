#!/usr/bin/env python3

from argparse import ArgumentParser
from collections import defaultdict

import shelve
import sys

argparser = ArgumentParser(description='''
  Based on the tweets in db/slice, compute pagerank scores for users
  and store them in db/userrank, overwriting any existing scores.
''')

argparser.add_argument('-n', '--toprint', default=10, type=int,
  help='how many top users to report on stdout')
argparser.add_argument('-a', '--alpha', default=0.15, type=float,
  help='pagerank taxation (i.e. in-flow)')
argparser.add_argument('-e', '--epsilon', default=0.001, type=float,
  help='error for convergence test')
argparser.add_argument('-g', '--dumpgraph', action='store_true',
  help='dump graph arcs to stderr')
argparser.add_argument('-r', '--dumpraw', action='store_true',
  help='dump graph arcs to stderr, adjacency lists')
args = None

# compress user ids to integers 0, 1, ...
los = [] # long_of_short
sol = {} # short_of_long

def register_userid(l):
  if l in sol:
    return
  sol[l] = len(los)
  los.append(l)

def dump_graph(g):
  global los, sol
  n = len(g)
  with shelve.open('db/users') as users:
    def name(x):
      if x == n - 1:
        return 'DUMMY'
      elif los[x] in users:
        return users[los[x]].screen_name
      else:
        return 'unknown-{}'.format(los[x])
    for s in range(n):
      for t, w in g[s]:
        sys.stderr.write('{:6.2f} {} {}\n'.format(w,name(s),name(t)))

def build_graph():
  global los, sol, args
  with shelve.open('db/slice') as tweets:
    for t in tweets.values():
      register_userid(t.author)
      for u in t.mention.users:
        register_userid(u)
  g = [defaultdict(int) for _ in range(len(los))]
  with shelve.open('db/slice') as tweets:
    for t in tweets.values():
      for u in t.mention.users:
        g[sol[t.author]][sol[u]] += 1
  if args.dumpraw:
    sys.stderr.write('{}\n'.format(len(g)))
    for d in g:
      for k, v in d.items():
        for _ in range(v):
          sys.stderr.write('{} '.format(k + 1))
      sys.stderr.write('0\n')
  for i in range(len(g)):
    g[i][i] = 0
  ng = []
  dummy = len(g)
  for oa in g:
    z = sum(oa.values())
    na = [(tgt, v/z*(1-args.alpha)) for tgt,v in oa.items() if v != 0]
    na.append((dummy, 1-sum(v for _, v in na)))
    ng.append(na)
  na = [(i, 1/len(g)) for i in range(len(g))]
  ng.append(na)
  return ng

def pagerank(g):
  n = len(g)
  nxt = [1]*n
  now = None
  error = args.epsilon + 1
  iterations = 0
  while error > args.epsilon:
    iterations += 1
    now, nxt = nxt, [0]*n
    for i in range(n):
      for j, f in g[i]:
        nxt[j] += now[i] * f
    error = max(abs(now[i]-nxt[i]) for i in range(n))
  sys.stderr.write('used {} iterations for {} users\n'.format(iterations, n))
  if not (0.99 * n < sum(now) < 1.01 * n):
    sys.stderr.write('W: numerical stability issues\n')
  return now

def save(scores, toprint):
  n = len(scores) - 1
  with shelve.open('db/userrank', 'n') as pr:
    for i in range(n):
      pr[los[i]] = scores[i]
  with shelve.open('db/users') as users:
    def sn(id):
      if id not in users:
        return 'unknown-{}'.format(id)
      else:
        return users[id].screen_name
    xs = sorted((-scores[i], sn(los[i])) for i in range(n))
    sys.stderr.write('lost flow {:.1f}\n'.format(scores[n]))
    for s, un in xs[:toprint]:
      sys.stdout.write('{:8.1f} https://twitter.com/{}\n'.format(-s, un))


def main():
  global args
  args = argparser.parse_args()
  g = build_graph()
  if args.dumpgraph:
    dump_graph(g)
  scores = pagerank(g)
  save(scores, args.toprint)

if __name__ == '__main__':
  main()
