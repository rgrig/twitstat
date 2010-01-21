# vim: set fileencoding=utf-8 :
# print the frequency of words appearing in the input

from contextlib import closing
from sys import argv, exit, stdin, stdout
import shelve
import string

letters = unicode(string.letters + '@-_#ăîÎșşȘțţȚŢâÂăĂ', 'utf-8')
romnice = unicode('ăîÎșşȘțţȚŢâÂăĂ', 'utf-8')
romugly = unicode('aiIssSttTTaAaA', 'utf-8')
vowels = 'aeiou'
invsoundexcode = ['mn']
soundexcode = dict()
for w in invsoundexcode:
  for c in w:
    soundexcode[c] = w[0]
for v in vowels:
  soundexcode[v] = ''
def secode(c):
  if c not in soundexcode:
    return c
  else:
    return soundexcode[c]
with open('stopwords', 'r') as f:
  stopwords = set([x.strip() for x in f.readlines()])

def ok(w):
  return len(w) >= 2

def romtrans(s):
  r = ''
  for c in s:
    i = romnice.find(c)
    if i == -1:
      r += c
    else:
      r += romugly[i]
  return r

def lettersplit(s):
  r = []
  w = ''
  for c in s:
    if c not in letters:
      if w != '':
        r.append(w)
        w = ''
    else:
      w += c
  if w != '':
    r.append(w)
  return r

def stopword(w):
  return False

def soundex(w):
  r = w[0]
  p = ''
  for l in w[1:]:
    c = secode(l)
    if c != p:
      r += c
    p = c
  return r

histo = dict()
variants = dict()
text = stdin.read()
text = unicode(text, 'utf-8')

for w in lettersplit(text):
  nw = romtrans(w)
  nw = nw.lower()
  if len(nw) < 3 or nw in stopwords:
    continue
  if len(argv) == 1:
    nw = soundex(nw)
  if nw not in variants:
    variants[nw] = set()
  variants[nw].add(w)
  if nw not in histo:
    histo[nw] = 1
  else:
    histo[nw] = histo[nw] + 1

lst = [(v, k) for k, v in histo.items()]
lst.sort()
lst.reverse()
for k, v in lst:
  stdout.write(str(k))
  for w in variants[v]:
    stdout.write(' ' + w.encode('utf-8'))
  stdout.write('\n')
