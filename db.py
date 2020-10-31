class User:
  def __init__(self, screen_name):
    self.screen_name = screen_name
    # TODO: self.is_trusted = False

class Mention:
  def __init__(self):
    self.users = set()
    self.urls = set()
    self.tweets = set()
  def __str__(self):
    result = '(users: ' + ' '.join(self.users) + ')'
    result += ' (urls: ' + ' '.join(self.urls) + ')'
    result += ' (tweets: ' + ' '.join(self.tweets) + ')'
    return result
  def as_dict(self):
    result = {}
    result['users'] = sorted(self.users)
    result['urls'] = sorted(self.urls)
    result['tweets'] = sorted(self.tweets)
    return result

class Tweet:
  def __init__(self, text, time, author, mention):
    self.text = text
    self.time = time
    self.author = author
    self.mention = mention
  def __str__(self):
    return 'author: {}\ntime: {}\nmention: {}\ntext: {}\n'.format(
      self.author, self.time, self.mention, self.text
    )
  def as_dict(self):
    return { 'text':self.text, 'time':self.time, 'author':self.author
            , 'mention':self.mention.as_dict() }
