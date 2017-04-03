class User:
  def __init__(self, screen_name):
    self.screen_name = screen_name

class Mention:
  def __init__(self):
    self.users = set()
    self.urls = set()
    self.tweets = set()

class Tweet:
  def __init__(self, text, time, author, mention):
    self.text = text
    self.time = time
    self.author = author
    self.mention = mention
