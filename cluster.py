#!/usr/bin/env python2
# vim: set fileencoding=utf-8 :

from contextlib import closing
from heapq import heappop, heappush
from numpy import zeros, ones, matrix
from random import randint
import shelve
from sys import argv, exit, stderr, stdin
from time import time

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

def make_undirected(dg, alpha):
  g = dict()
  for i in xrange(len(dg)):
    g[i] = dict()
  for src in xrange(len(dg)):
    tgts = dg[src]
    for tgt in tgts:
      if tgt not in g[src]:
        g[src][tgt] = g[tgt][src] = 0
      g[src][tgt] += 1.0 / len(tgts)
      g[tgt][src] += 1.0 / len(tgts)
  for i in xrange(1, len(g)):
    g[0][i] = g[i][0] = alpha
  return g

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

# See Flake et al. 2004.
def min_cut_clustering(g):
  stderr.write('clustering {0} users\n'.format(len(g)))
  t1 = time()
  boss = range(len(g))
  nodes = range(1, len(g))
  def weight_sum(d):
    r = 0
    for w in d.itervalues():
      r += w
    return r
  nodes.sort(lambda x, y: cmp(weight_sum(g[y]), weight_sum(g[x])))
  touched = set()
  for x in nodes:
    if x in touched:
      continue
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
  clusters = dict()
  for i in xrange(1, len(g)):
    rep = find(boss, i)
    if rep not in clusters:
      clusters[rep] = set()
    clusters[rep].add(i)
  return clusters.values()

# PageRank.
def order_cluster(dg, cluster):
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
    ys = [0]
    for y in dg[x]:
      if y in cluster:
        ys.append(y)
    for y in ys:
      G[compressed[y], compressed[x]] = 1.0 / len(ys)
    G[compressed[x], 0] = 1.0 / len(cluster)

  # iterate
  t0 = time()
  c = ones(n)
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

def main():
  alpha = parse_command()
  name_of_index, orig_graph = parse_graph()
  graph = make_undirected(orig_graph, alpha)
  clusters = min_cut_clustering(graph)
  clusters.sort(lambda x, y: cmp(len(y), len(x)))
  with open('groups.txt', 'w') as file:
    for us in clusters:
      ous = order_cluster(orig_graph, us)
      file.write(str(len(ous)))
      for u in ous:
        file.write(' ')
        file.write(name_of_index[u])
      file.write('\n')

if __name__ == '__main__':
  main()
