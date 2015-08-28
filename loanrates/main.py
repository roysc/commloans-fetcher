#!/bin/python3

import sys
from loanrates import fetch

def main(args):
  if len(args) < 3:
    print('Usage:', args[0], 'target_dir', 'state_code', '[county_code]')
    sys.exit(1)
    
  f = fetch.Fetcher(args[1])
  if len(args) == 3:
    f.request_all_counties(args[2])
  else:
    f.request_all_years(args[2], args[3])

  return 0

if __name__ == '__main__':
  main(sys.argv)
