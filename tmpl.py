#!/usr/bin/env python2

from sys import argv, stdin, stdout

todo = [
    'WORDS_TOP=words_top.html',
    'URLS_TOP=urls_top.html',
    'USERS_TOP=users_top.html']

if len(argv) > 1:
  todo = argv[1:]

text = stdin.read()
for a in todo:
  kw, fn = a.split('=')
  with open(fn, 'r') as f:
    text = text.replace(kw, f.read())
stdout.write(text)
