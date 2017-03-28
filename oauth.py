#!/usr/bin/env python3

from base64 import b64encode

import requests

URL = 'https://api.twitter.com/oauth2/token'

def login():
  secrets = {}
  with open('secret.credentials') as f:
    for line in f:
      ws = line.split()
      secrets[ws[0]] = ws[1]

  consumer_key = secrets['consumer-key']
  consumer_sec = secrets['consumer-secret']
  key = b64encode(bytes('{}:{}'.format(consumer_key, consumer_sec), encoding='utf8'))

  headers = \
    { 'Authorization' : 'Basic {}'.format(str(key, encoding='utf8'))
    , 'Content-Type' : 'application/x-www-form-urlencoded;charset=UTF-8' }
  data = 'grant_type=client_credentials'
  r = requests.post(URL, data=data, headers=headers).json()
  if r['token_type'] != 'bearer':
    sys.stderr.write('cannot understand login reply: {}\n'.format(r))
    return
  secrets['access-token'] = r['access_token']

  with open('secret.credentials', 'w') as f:
    for k, v in sorted(secrets.items()):
      f.write('{} {}\n'.format(k, v))

if __name__ == '__main__':
  login()
