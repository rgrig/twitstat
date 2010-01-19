# Searches for a regular expression in the input and
# for each match it prints a line with the groups

from re import finditer
from sys import argv, exit, stdin, stderr, stdout

def go(s):
  for m in finditer(argv[1], s):
    for g in m.groups():
      stdout.write(g)
      stdout.write(' ')
    stdout.write('\n')

if len(argv) < 2:
  stderr.write('Please provide a regular expression.\n')
  exit(1)

if len(argv) == 2:
  go(stdin.read())
else:
  for fn in argv[2:]:
    with open(fn, 'r') as f:
      go(f.read())

