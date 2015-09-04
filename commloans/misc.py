import pandas as pd
from datetime import datetime, timedelta

from commloans import county_codes as cc

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

def read_harvest_dates(f):

  def splitdm(col):
    s = col.str.strip()
    s = s.str.split()
    l = []
    for m, d in s:
      l.append((datetime.strptime(m, '%b').month, int(d)))
    return pd.Series(l, index=s.index)

  d = pd.read_csv(f,index_col=0)
  start = splitdm(d['start'])
  end = splitdm(d['end'])
  ranges = pd.concat({'start':start,'end':end},axis=1)
  
  return ranges


def ez_harvest_price_mean(alldata, st, h):
  
  dates = h.loc[st]
  d = alldata[st]
  i = yearly_dates(d.index, dates['start'], dates['end'])
  d = d.ix[i]
  g = d.groupby(d.index.year)
  return g.mean()
