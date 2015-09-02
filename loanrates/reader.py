import os
import pandas as pd
from loanrates import county_codes

column_names = [
  'commodity',
  'date',
  'loan_rate',
  'pcp_30day',
  'pcp_alternative',
  'effective_pcp',
  'effective_ldp',
  'effective_acre_ldp',
]

# keep_columns = (
# 'loan_rate', 'effective_pcp', 'effective_ldp'
# )

drop_columns = [
  # Commodity always == "Corn"
  'commodity',
  # These are always(?) NaN
  'pcp_30day',
  'pcp_alternative',
  'effective_acre_ldp',
]

def read_csv(fp):
  from itertools import islice

  d = pd.read_csv(fp,
                  skiprows=4,
                  parse_dates=['Effective Date'])
  d.columns = column_names
  d.set_index('date', inplace=True)

  # Drop unneeded columns
  d = d.drop(drop_columns, axis=1)

  # Strip $, convert to float
  def parsemoney(c):
    return c.str.strip('$ ').astype(float)
  d = d.apply(parsemoney)
  
  return d

def get_state_county(fp):
  fp = open(path)
  descr = next(islice(fp, 3, None))
  # ...


class Reader:
  def __init__(self, root):
    self.root= root


  def process_all_files(self, state, county):
    path = os.path.join(self.root, state, county)

    dfs = []
    for fn in os.listdir(path):
      fpath = os.path.join(path, fn)
      print("reading:", fpath)
      d = read_csv(fpath)
      # Use multi-index for better organization
      index = pd.MultiIndex.from_product([[state], [county], d.columns])
      d.columns = index
      assert not d.isnull().any().any(), d
      dfs.append(d)

    # Deal with empty directory
    if not dfs:
      return pd.DataFrame()
    
    ret = pd.concat(dfs)
    # Drop duplicates
    ret = ret.groupby(level=0).first()
    ret.sort(inplace=True)
    return ret

  def process_all_counties(self, state):
    path = os.path.join(self.root, state)
    statename = county_codes.state_names[state]
    counties = county_codes.counties[statename]
    print("reading dir:", path)
    dfs = []
    for fn in os.listdir(path):
      if fn not in counties:
        print('bad county code:', fn)
      d = self.process_all_files(state, fn)
      assert not d.isnull().any().any(), d
      dfs.append(d)

    ret = pd.concat(dfs, axis=1)
    ret.sort(axis=1, inplace=True)
    return ret
