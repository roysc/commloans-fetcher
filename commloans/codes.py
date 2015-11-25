import inspect, os
import pandas as pd

def _get_counties():
  fr = inspect.currentframe()
  me = os.path.realpath(inspect.getfile(fr))
  path = os.path.join(os.path.dirname(me), 'data', 'counties.csv')
  
  return pd.read_csv(path)

counties = _get_counties()
