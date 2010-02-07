#!/usr/bin/env python2

from sys import argv, stdin, stdout

text = stdin.read()
for a in argv[1:]:
  kw, fn = a.split('=')
  with open(fn, 'r') as f:
    text = text.replace(kw, f.read())
stdout.write(text)
