#!/usr/bin/env python2
# vim: set fileencoding=utf-8 :

from heapq import heappop, heappush
from random import randint
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
  graph = dict()
  names_count = 0
  index_of_name = dict()
  with open('talkgraph.txt', 'r') as file:
    for line in file:
      xs = line.split()
      for x in [xs[0]] + xs[2:]:
        if x not in index_of_name:
          names_count += 1
          index_of_name[x] = names_count
          graph[names_count] = set()
      graph[index_of_name[xs[0]]].update([index_of_name[n] for n in xs[2:]])
  names_count += 1
  name_of_index = names_count * ['']
  name_of_index[0] = '*ARTIFICIAL*'
  for n, i in index_of_name.iteritems():
    name_of_index[i] = n
  for src, tgts in graph.iteritems():
    tgts.discard(src)
  return (name_of_index, graph)

def make_undirected(dg, alpha):
  g = dict([(i, dict())for i in xrange(len(dg)+1)])
  for src, tgts in dg.iteritems():
    for tgt in tgts:
      if tgt not in g[src]:
        g[src][tgt] = 1.0 / len(tgts)
        if src in dg[tgt]:
          g[src][tgt] += 1.0 / len(dg[tgt])
        g[tgt][src] = g[src][tgt]
  for i in xrange(1, len(g)):
    g[0][i] = g[i][0] = alpha
  return g

def find(boss, x):
  if boss[x] == x:
    return x
  boss[x] = find(boss, boss[x])
  return boss[x]

def union(boss, x, y):
  if randint(0, 1) == 1:
    x, y = y, x
  boss[find(boss, x)] = find(boss, y)

def max_cut_clustering(g):
  stderr.write('processing {0} users\n'.format(len(g)))
  t1 = time()
  boss = range(len(g))
  nodes = range(1, len(g))
  nodes.sort(lambda x, y: cmp(len(g[y]), len(g[x])))
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
      stderr.write('{0: >4.0%} done\n'.format(float(len(touched))/len(g)))
  clusters = dict()
  for i in xrange(1, len(g)):
    rep = find(boss, i)
    if rep not in clusters:
      clusters[rep] = set()
    clusters[rep].add(i)
  return clusters.values()

def order_cluster(g, cluster):
  dist_sum = dict()
  for x in cluster:
    dist = dict()
    q = [(x, 0)]
    while len(q) > 0:
      d, y = heappop(q)
      if y in dist:
        continue
      dist[y] = d
      for z, w in g[y].iteritems():
        if z not in dist and z in cluster:
          heappush(q, (w + d, z))
    sum = 0
    for y in dist.itervalues():
      sum += y
    dist_sum[x] = sum
  result = list(cluster)
  result.sort(lambda x, y: cmp(dist_sum[x], dist_sum[y]))
  return result

def main():
  alpha = parse_command()
  name_of_index, orig_graph = parse_graph()
  graph = make_undirected(orig_graph, alpha)
  clusters = max_cut_clustering(graph)
  clusters.sort(lambda x, y: cmp(len(y), len(x)))
  with open('groups.txt', 'w') as file:
    for us in clusters:
      ous = order_cluster(graph, us)
      file.write(str(len(ous)))
      for u in ous:
        file.write(' ')
        file.write(name_of_index[u])
      file.write('\n')

if __name__ == '__main__':
  main()
