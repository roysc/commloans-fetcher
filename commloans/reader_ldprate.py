import os
import pandas as pd
from commloans import county_codes
from commloans._reader import Reader

column_names = [
  'commodity',
  'date',
  'loanrate',
  'pcp_30day',
  'pcp_alternative',
  'pcp',
  'effective_ldp',
  'effective_acre_ldp',
]

# keep_columns = (
# 'loanrate', 'pcp', 'effective_ldp'
# )

drop_columns = [
  # Commodity always == "Corn"
  'commodity',
  # These are always(?) NaN
  'pcp_30day',
  'pcp_alternative',
  'effective_ldp',
  'effective_acre_ldp',
]

def read_csv_usda(fp):
  "Read a CSV from the USDA ACR web app"
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


class LDPRateReader(Reader):

  def process_all_files(self, state, county):
    path = os.path.join(self.root, str(state), county)
    dfs = []
    for file in os.listdir(path):
      if not file.endswith('csv'):
        continue
      fpath = os.path.join(path, file)
      print("reading:", fpath)
      d = read_csv_usda(fpath)
      # Use multi-index for better organization
      index = pd.MultiIndex.from_product([d.columns, [int(state)], [int(county)]])
      d.columns = index
      # assert not d.isnull().any().any(), d
      dfs.append(d)

    # Deal with empty directory
    if not dfs:
      return pd.DataFrame()
    
    ret = pd.concat(dfs)
    # Drop duplicates
    ret = ret.groupby(level=0).first()
    ret.sort_index(inplace=True)
    return ret

  def process_all_counties(self, state):
    path = os.path.join(self.root, str(state))
    # statename = county_codes.state_names[state]
    counties = county_codes.counties[state]
    print("reading dir:", path)
    dfs = []
    files = os.listdir(path)
    for cty in counties:
      if cty not in files:
        print('county dir not found:', cty)
        continue
      d = self.process_all_files(state, cty)
      # assert not d.isnull().any().any(), d
      dfs.append(d)

    ret = pd.concat(dfs, axis=1)
    ret.sort_index(axis=1, inplace=True)
    return ret

  def process_all_states(self):
    states = sorted(county_codes.state_names.keys())
    dfs = []
    missing = []
    for s in states:
      path = os.path.join(self.root, str(s))
      if not os.path.exists(path):
        missing.append(s)
    if missing:
      raise RuntimeError("missing states", missing)
        
    for s in states:
      d = self.process_all_counties(s)
      dfs.append(d)
    ret = pd.concat(dfs, axis=1)
    ret.sort_index(axis=1, inplace=True)
    return ret
      

def read_county_stata(path):
  assert path.endswith('.dta'), "not Stata file"
  d = pd.read_stata(path)
  d.set_index(['stateansi','countyansi','year'], inplace=True)
  d.dropna(subset='countyansi', inplace=True)
  return d
