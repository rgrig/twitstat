#!/usr/bin/env python3

from argparse import ArgumentParser

argparser = ArgumentParser(description='''
  Switch database.
''')

argparser.add_argument('newdb', nargs='?',
  help='name of new database')

def main():
  pass

if __name__ == '__main__':
  main()
