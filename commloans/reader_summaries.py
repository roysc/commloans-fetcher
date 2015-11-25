import os
import pandas as pd, numpy as np
from commloans import codes, county_codes
from commloans._reader import Reader


def _get_counties():
  c = codes.counties.copy()
  # Apostrophes not in data counties
  c['county_name'] = c.county_name.str.replace("'", '')
  ixcols = ['state', 'county_name']
  c.drop_duplicates(ixcols, inplace=1)
  c.set_index(ixcols, inplace=1)
  c.sort_index(inplace=1)
  return c
  
counties = _get_counties()


def process_csv(f):
  d = pd.read_csv(f, skiprows=4, )
  cols = 'year county_name comm unit ct qty amt'.split()
  d.columns = cols
  for c in ['ct','qty','amt']:
    if d[c].dtype == np.object_:
      d[c] = pd.to_numeric(d[c].str.replace(',',''))
  return d

class SummariesReader(Reader):

  def process_all_files(self, state):
    dpath = os.path.join(self.root, str(state))
    dfs = []
    for file in os.listdir(dpath):
      if not file.endswith('csv'):
        continue
      fpath = os.path.join(dpath, file)
      print("reading:", fpath)
      d = process_csv(fpath)
      d['state'] = state
      dfs.append(d)

    # Deal with empty directory
    if not dfs:
      return pd.DataFrame()
    
    ret = pd.concat(dfs)
    # Drop duplicates
    # ret = ret.groupby(level=0).first()
    ret.sort_index(inplace=True)
    return ret
  
  def process_all_states(self):
    states = sorted(county_codes.state_names.keys())
    for s in [60,2]:
      states.remove(s)
    dfs = []
    missing = []
    for s in states:
      path = os.path.join(self.root, str(s))
      if not os.path.exists(path):
        missing.append(s)
    if missing:
      raise RuntimeError("missing states", missing)
        
    for s in states:
      d = self.process_all_files(s)
      dfs.append(d)
    ret = pd.concat(dfs, axis=0)

    renames = {
      (17, 'LA SALLE'): 'LASALLE',
      (None, 'DE KALB'): 'DEKALB',
      (19, 'WEST POTTAWATTAMIE'): 'POTTAWATTAMIE',
      (23, 'HOULTON'): 'AROOSTOOK',   # 
      (23, 'FORT KENT'): 'AROOSTOOK', # 
      (27, 'EAST POLK'): 'POLK',
      (27, 'NORTH ST. LOUIS'): 'ST. LOUIS',
      (27, 'WEST OTTER TAIL'): 'OTTER TAIL',
      (27, 'WEST POLK'): 'POLK',
      (32, 'CARSON CITY'): 'CARSON',
      (39, 'EAST LUCAS'): 'LUCAS',
      (39, 'WEST LUCAS'): 'LUCAS',
    }
    for s, c in renames:
      rix = (ret['county_name'] == c)
      if s is not None:
        rix &= (ret['state'] == s)
      to = renames[(s, c)]
      if to is None:
        ret = ret.loc[~rix]
      else:
        ret.loc[rix, 'county_name'] = to

    bad = set()
    def ccode(row):
      try:
        return counties.loc[(row['state'], row['county_name']), 'county']
      except KeyError as e:
        bad.add(e.args)

    ret['county'] = ret.apply(ccode, axis=1)
    # return ret
    
    ixcols = ['state', 'county', 'year']
    ret.drop(['unit', 'county_name'], axis=1, inplace=1)
    # Account for remapped names by taking sum
    ret = ret.groupby(['comm'] + ixcols).sum()
    ret = ret.reset_index().set_index(ixcols)
    
    g = ret.groupby('comm')    # level 0 is comm
    
    comms = {
      'CORN':'corn', 'SORG':'sorghum', 'WHT':'wheat', 'SOYA':'soy', 'OATS':'oats'
    }
    ds = {v: g.get_group(k).drop('comm',axis=1) for k, v in comms.items()}
    ret = pd.concat(ds, axis=1).sort_index()

    return ret
