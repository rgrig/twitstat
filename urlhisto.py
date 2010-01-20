# Ranks the URLs appearing in the input by they count.

from re import finditer
from sys import argv, exit, stdin, stderr, stdout

def opt(s):
  return '(' + s + ')?'
def lst(s):
  return '(' + s + ')*'
def alt(l):
  return '(' + '|'.join(['('+x+')' for x in l]) + ')'

# from RFC1738
hex = '[0-9a-fA-F]'
escape = '%' + hex + hex
unreserved = r'[a-zA-Z0-9\$_\.\+!\*\'\(\),-]'
uchar = alt([unreserved, escape])
hsegment = lst(alt([uchar, '[;:@&=]']))
hpath = hsegment + lst('/' + hsegment)
hostnumber = r'[0-9]\.[0-9]\.[0-9]\.[0-9]'
toplabel = '[a-zA-Z]' + opt(lst('[a-zA-Z0-9]|-') + '[a-zA-Z0-9]')
domainlabel = '[a-zA-Z0-9]' + opt(lst('[a-zA-Z0-9]|-') + '[a-zA-Z0-9]')
hostname = lst(domainlabel + r'\.') + toplabel
host = alt([hostname, hostnumber])
hostport = host + opt(':[0-9]+')
http = 'http://' + hostport + opt('/' + hpath + opt(r'\?' + hsegment))

cnt = dict()

for m in finditer(http, stdin.read()):
  url = m.group()
  if url not in cnt:
    cnt[url] = 1
  else:
    cnt[url] += 1
lst = [(v, k) for k, v in cnt.items()]
lst.sort()
lst.reverse()
for k, v in lst:
  stdout.write('{0}\t{1}\n'.format(k, v.encode('utf-8')))
