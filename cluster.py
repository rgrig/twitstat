#!/usr/bin/env python2
# vim: set fileencoding=utf-8 :

from collections import deque
from contextlib import closing
from heapq import heappop, heappush
from numpy import zeros, ones, matrix
from random import randint
import shelve
from sys import argv, exit, stderr, stdin, stdout
from time import time

# Reading guide:
#   g   is a graph
#   dg  is a digraph

#{{{ union-find (randomized and with path compression)
def find(boss, x):
  y = x
  while y != boss[y]:
    y = boss[y]
  while x != y:
    boss[x], x = y, boss[x]
  return y

def union(boss, x, y):
  if randint(0, 1) == 1:
    x, y = y, x
  boss[find(boss, x)] = find(boss, y)
#}}}

def parse_command():
  if len(argv) != 2:
    stderr.write('usage: cluster.py alpha\n')
    exit(1)
  try:
    return float(argv[1])
  except:
    stderr.write('The argument should be a number.\n')
    exit(2)

def parse_graph():
  # Give numbers to names (to speed up the graph algos).
  all_names = set()
  with closing(shelve.open('histograms', 'r')) as f:
    for src in f.keys():
      all_names.add(src)
      for tgt in f[src]['mentions'].iterkeys():
        all_names.add(tgt)
  index_of_name = dict()
  name_of_index = ['*ARTIFICIAL*']
  names_count = 0
  for n in all_names:
    if n not in index_of_name:
      names_count += 1
      index_of_name[n] = names_count
      name_of_index.append(n)
  names_count += 1
  words_of_user = [dict()]
  with closing(shelve.open('histograms', 'r')) as f:
    for n in name_of_index[1:]:
      if n in f:
        words_of_user.append(f[n]['words'])
      else:
        words_of_user.append(dict())

  # Get the graph, and use numbers to represent it.
  graph = [dict() for _ in xrange(names_count)]
  with closing(shelve.open('histograms', 'r')) as f:
    for src in f.keys():
      src_dict = graph[index_of_name[src]]
      for tgt, w in f[src]['mentions'].iteritems():
        if tgt != src:
          src_dict[index_of_name[tgt]] = w
  return (words_of_user, name_of_index, graph)

def make_undirected(dg, boss, alpha):
  g = dict()
  g[0] = dict()
  for x in xrange(1, len(dg)):
    y = find(boss, x)
    if y not in g:
      g[y] = dict([(0,0)])
      g[0][y] = 0
    g[y][0] += alpha
    g[0][y] += alpha
  for x in xrange(1, len(dg)):
    src = find(boss, x)
    tw = 0
    for w in dg[x].itervalues():
      tw += w
    for y, w in dg[x].iteritems():
      tgt = find(boss, y)
      if src == tgt:
        continue
      if tgt not in g[src]:
        g[src][tgt] = g[tgt][src] = 0.0
      g[src][tgt] += 1.0 * w / tw
      g[tgt][src] += 1.0 * w / tw
  return g

# See Flake et al. 2004.
def cut_clustering(g, boss, name_of_index):
  stderr.write('clustering {0} nodes\n'.format(len(g)))
  t1 = time()
  total_weight = dict()
  for x, ys in g.iteritems():
    if x != 0:
      total_weight[x] = 0
      for w in ys.itervalues():
        total_weight[x] -= w
  nodes = [(w, x) for (x, w) in total_weight.iteritems()]
  nodes.sort()
  touched = set()
  for _, x in nodes:
    if x in touched:
      continue
    # max flow / min cut
    rn = dict([(src, dict(tgts)) for (src, tgts) in g.iteritems()])
    while True:
      pred = dict()
      seen = set([x])
      q = deque([x])
      while len(q) > 0:
        y = q.popleft()
        if y == 0:
          break
        for z, w in rn[y].iteritems():
          if w > 0 and z not in seen:
            seen.add(z)
            q.append(z)
            pred[z] = y
      if y != 0:  # no new path found
        break
      w = float('inf')
      #stderr.write('add path')
      while y != x:
        #stderr.write(' {0}({1})'.format(y, g[pred[y]][y]))
        w, y = min(w, g[pred[y]][y]), pred[y]
      #stderr.write(' [{0}]\n'.format(w))
      y = 0
      while y != x:
        rn[pred[y]][y] -= w
        rn[y][pred[y]] += w
        y = pred[y]
    touched.update(seen)
    for y in seen:
      union(boss, x, y)
    t2 = time()
    if t2 - t1 > 10:
      t1 = t2
      stderr.write('  {0: >4.0%} clustered\n'.format(float(len(touched))/len(g)))

# iterative pagerank
def order_cluster(dg, cluster):
  REP_LIMIT = 100
  if len(cluster) > REP_LIMIT:
    stderr.write('ranking {0} users... '.format(len(cluster)))
    t1 = time()
  g = dict([(x, dict()) for x in cluster])
  g[0] = dict()
  for x in cluster:
    tw = 1  # for the edge going to 0
    for y, w in dg[x].iteritems():
      if y in cluster:
        tw += w
    g[0][x] = 1.0 / tw
    for y, w in dg[x].iteritems():
      if y in cluster:
        g[y][x] = 1.0 * w / tw
  for x in cluster:
    g[x][0] = 1.0 / len(cluster)

  score = dict.fromkeys(g.iterkeys(), 1.0)
  new_score = dict.fromkeys(g.iterkeys(), 0.0)
  for i in xrange(1000):
    if len(cluster) > REP_LIMIT:
      if time() - t1 > 10:
        stderr.write('stoping early after {0} iterations... '.format(i))
        break
    for x, ys in g.iteritems():
      for y, w in ys.iteritems():
        new_score[x] += score[y] * w
    score = new_score
    new_score = dict.fromkeys(g.iterkeys(), 0.0)

  result = list(cluster)
  result.sort(lambda x, y: cmp(score[y], score[x]))
  if len(cluster) > REP_LIMIT:
    stderr.write('done in {0:.2f} seconds\n'.format(time()-t1))
  return result

def compute_children(old_boss, new_boss):
  assert len(old_boss) == len(new_boss)
  c = dict()
  for x in xrange(1, len(new_boss)):
    ob = find(old_boss, x)
    nb = find(new_boss, x)
    if nb not in c:
      c[nb] = set()
    c[nb].add(ob)
  return c

def get_cluster(children, level, x):
  if level >= len(children):
    return set([x])
  r = set()
  for y in children[level][x]:
    r |= get_cluster(children, level + 1, y)
  return r

def describe_cluster(words_of_user, cluster):
  interesting_words = set()
  for u in cluster:
    for w in words_of_user[u].iterkeys():
      interesting_words.add(w)
  inside = dict([(w,0) for w in interesting_words])
  outside = dict([(w,0) for w in interesting_words])
  for u in xrange(1, len(words_of_user)):
    if u in cluster:
      d = inside
    else:
      d = outside
    for w in interesting_words:
      if w in words_of_user[u]:
        d[w] += words_of_user[u][w]
  h1 = []
  h2 = []
  for w in interesting_words:
    if outside[w] == 0:
      heappush(h1, (-inside[w], w))
    else:
      heappush(h2, (-1.0*inside[w]/outside[w], w))
  result = []
#  while len(h1) > 0 and len(result) < 5:
#    _, w = heappop(h1)
#    result.append(w)
  while len(h2) > 0 and len(result) < 5:
    _, w = heappop(h2)
    result.append(w)
  return result

def print_clusters(words_of_user, name_of_index, orig_graph, children, pl, level, root):
  if level >= len(children):
    return
  clusters = []
  for x in children[level][root]:
    one_cluster = get_cluster(children, level + 1, x)
    if len(one_cluster) >= 20:
      clusters.append((x, one_cluster))
  if len(clusters) == 1:
    print_clusters(words_of_user, name_of_index, orig_graph, children, pl, level + 1, clusters[0][0])
    return
  clusters.sort(lambda x, y: cmp(len(y[1]), len(x[1])))
  for x, c in clusters:
    oc = order_cluster(orig_graph, c)
    ws = describe_cluster(words_of_user, c)
    stdout.write('  ' * pl)
    stdout.write(str(len(oc)))
    stdout.write(', in frunte cu')
    for y in oc[:5]:
      stdout.write(' ')
      stdout.write(name_of_index[y])
    stdout.write(', au vorbit despre')
    for w in ws:
      stdout.write(' ')
      stdout.write(w)
    stdout.write('\n')
    print_clusters(words_of_user, name_of_index, orig_graph, children, pl + 1, level + 1, x)

def main():
  words_of_user, name_of_index, orig_graph = parse_graph()
  boss = range(len(orig_graph))
  children = []
  for alpha in [0.1, 0.01, 0.005, 0]:
    graph = make_undirected(orig_graph, boss, alpha)
    old_boss = [x for x in boss]
    cut_clustering(graph, boss, name_of_index)
    children.append(compute_children(old_boss, boss))
  children.append(compute_children(boss, [0 for _ in boss]))
  children.reverse()
  print_clusters(words_of_user, name_of_index, orig_graph, children, 0, 0, 0)

if __name__ == '__main__':
  main()
