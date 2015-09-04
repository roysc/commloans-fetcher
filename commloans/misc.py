import pandas as pd
from datetime import datetime, timedelta

def ez_read_csv(f):
  d = pd.read_csv(f, header=[0,1], parse_dates=0, index_col=0)
  levs = d.columns.levels
  ilevs = [[int(s) for s in l] for l in levs]
  d.columns.set_levels(ilevs,inplace=True)
  return d

def yearly_dates(ix, startmd, endmd):
  "Get year-offset datetimeindex for month and day"
  m0, d0 = startmd
  m1, d1 = endmd
  fmt = '{}-{:02}-{:02}'

  def getdates(y):
    start = fmt.format(y, m0, d0)
    end = fmt.format(y, m1, d1)
    return (start <= ix) & (ix <= end)

  years = ix.groupby(ix.year).keys()
  ret = pd.Series(index=ix)
  ret[:] = False
  for y in years:
    ret = ret | getdates(y)
  return ret
