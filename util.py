from time import perf_counter

import sys

last_time = perf_counter()

def phase(m):
  global last_time
  now = perf_counter()
  sys.stderr.write('PHASE {:.1f} {}\n'.format(now - last_time, m))
  last_time = now
