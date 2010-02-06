#!/usr/bin/env python2
# vim: set fileencoding=utf-8 :

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
  with closing(shelve.open('clustering.shelf')) as f:
    for src in f.keys():
      all_names.add(src)
      for tgt in f[src][1]:
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

  # Get the graph, and use numbers to represent it.
  graph = [[]] * names_count
  with closing(shelve.open('clustering.shelf')) as f:
    for src in f.keys():
      src_idx = index_of_name[src]
      tgts = set([index_of_name[n] for n in f[src][1]])
      tgts.discard(src_idx)
      graph[src_idx] = list(tgts)
  return (name_of_index, graph)

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
  for x in xrange(len(dg)):
    src = find(boss, x)
    ys = dg[x]
    for y in ys:
      tgt = find(boss, y)
      if src == tgt:
        continue
      if tgt not in g[src]:
        g[src][tgt] = g[tgt][src] = 0.0
      g[src][tgt] += 1.0 / len(ys)
      g[tgt][src] += 1.0 / len(ys)
  return g

# See Flake et al. 2004.
def cut_clustering(g, boss):
  stderr.write('clustering {0} users\n'.format(len(g)))
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
      q = [x]
      while len(q) > 0:
        y, q = q[0], q[1:]
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

def pagerank(dg, cluster):
  if len(cluster) > 99:
    cnt = 0
    t1 = time()
    stderr.write('ordering cluster of {0} users'.format(len(cluster)))
  # map cluster elements to 1..n
  n = 0
  compressed = dict()
  compressed[0] = 0
  for x in cluster:
    n += 1
    compressed[x] = n
  n += 1
  
  # create the adjacency matrix
  G = matrix(zeros((n,n)))
  for x in cluster:
    for y in dg[x]:
      if y in cluster:
        G[compressed[y], compressed[x]] = 1.0
    G[compressed[x], 0] = G[0, compressed[x]] = 1.0

  # iterate
  t0 = time()
  c = ones(n)
  G = G / (c * G)
  for i in xrange(10):
    if time() - t0 > 3:
      if i > 3:
        break
      if len(cluster) > 99:
        stderr.write('.')
    G = G * G
    G = G / (c * G)
  score = G * ones((n,1))

  result = list(cluster)
  result.sort(lambda x, y: cmp(score[compressed[y]], score[compressed[x]]))
  if len(cluster) > 99:
    stderr.write('\n')
  return result

def simple_cluster_ordering(dg, cluster):
  weight = dict()
  for x in cluster:
    weight[x] = 1
  for tgts in dg:
    for y in tgts:
      if y in cluster:
        weight[y] += 1.0 / len(tgts)
  result = list(cluster)
  result.sort(lambda x, y: cmp(weight[y], weight[x]))
  return result

def order_cluster(dg, cluster):
  if len(cluster) > 3000:
    stderr.write('quickly ordering cluster of {0}\n'.format(len(cluster)))
    return simple_cluster_ordering(dg, cluster)
  else:
    return pagerank(dg, cluster)

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

def print_clusters(name_of_index, orig_graph, children, level, root):
  if level >= len(children):
    return
  clusters = []
  for x in children[level][root]:
    one_cluster = get_cluster(children, level + 1, x)
    if len(one_cluster) >= 5:
      clusters.append((x, one_cluster))
#  if len(clusters) <= 1:
#    return
  clusters.sort(lambda x, y: cmp(len(y[1]), len(x[1])))
  for x, c in clusters:
    oc = order_cluster(orig_graph, c)
    stdout.write('  ' * level)
    stdout.write(str(len(oc)))
    stdout.write(', in frunte cu')
    for y in oc[:5]:
      stdout.write(' ')
      stdout.write(name_of_index[y])
    stdout.write('\n')
    print_clusters(name_of_index, orig_graph, children, level + 1, x)

def main():
  name_of_index, orig_graph = parse_graph()
  boss = range(len(orig_graph))
  children = []
  for alpha in [1, 0.1, 0.01, 0]:
    graph = make_undirected(orig_graph, boss, alpha)
    old_boss = [x for x in boss]
    cut_clustering(graph, boss)
    children.append(compute_children(old_boss, boss))
  children.append(compute_children(boss, [0 for _ in boss]))
  children.reverse()
  print_clusters(name_of_index, orig_graph, children, 0, 0)

if __name__ == '__main__':
  main()
