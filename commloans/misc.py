import os
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import commloans.county_codes as cc
statecodes = sorted(cc.state_names.keys())


def ez_read1(f):
  d = pd.read_csv(f, header=[0,1], parse_dates=0, index_col=0)
  levs = d.columns.levels
  ilevs = [[int(s) for s in l] for l in levs]
  d.columns.set_levels(ilevs,inplace=True)
  return d

def ez_read2(f):
  d = pd.read_csv(f, header=[0,1,2], skiprows=[3],index_col=0, parse_dates=0)
  levs = d.columns.levels
  ilevs = levs[:1] + [[int(s) for s in l] for l in levs[1:]]
  d.columns.set_levels(ilevs,inplace=True)
  return d

def read_dates_simple(f):
  "Read lists of harvest prices"
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

def read_dates(f):
  import re
  rxdates = "{d} +{d} - {d} +{d}".format(d=r"(\w{3}) (\d{1,2})")
  rx = r"(?P<state>\w+) \.*: *\S+ +%s +%s" % (rxdates, rxdates)
  rx = re.compile(rx)
  
  mi = pd.MultiIndex.from_product([['plant','harvest'], ['start','end']])
  d = pd.DataFrame(columns=mi, index=statecodes)
  with open(f) as file:
    for line in file:
      m = rx.match(line)
      if m:
        groups = m.groups()
        dates = []
        positions = [0, 3, 4, 7] # positions of start-end dates
        for pos in positions:
          i = 1 + 2*pos
          mon, day = groups[i:i+2]
          dates.append((datetime.strptime(mon, '%b').month, int(day)))
        sc = cc.state_codes[m.group('state').upper()]
        d.ix[sc] = dates
  d.dropna(inplace=1)
  return d

def yearly_intervals(ix, indexer):
  "Get yearly intervals from sub-year date ranges"
  years = ix.groupby(ix.year).keys()
  ret = pd.DataFrame(index=ix,columns=['mask','bucket'])
  ret['mask'] = False
  for y in years:
    also = indexer(y, ix)
    ret['mask'] |= also
    ret.loc[also, 'bucket'] = y
  return ret


def annual_startend(start, end):
  m0, d0 = start
  m1, d1 = end
  def f(y, ix):
    a = datetime(y, m0, d0)
    b = datetime(y, m1, d1)
    return (a <= ix) & (ix <= b)
  return f

def price_mean(data, dates, st, planting):
  """Calculate price mean in a certain range of dates
  planting: whether to calculate planting price (and include June '04 data)
  """
  kind = 'plant' if planting else 'harvest'
  dates = dates.loc[st, kind]
  d = data[st]
  f = annual_startend(dates['start'], dates['end'])
  i = yearly_intervals(d.index, f)
  if planting:
      i['mask'] |= (('2004-06-01' <= i.index) & (i.index <= '2004-07-01'))
  d = d.ix[i['mask']]
  g = d.groupby(d.index.year)
  return g.mean()

def annual_startplus(start, plus):
  m0, d0 = start
  def f(y, ix):
    a = datetime(y, m0, d0)
    b = a + relativedelta(months=plus)
    return (a <= ix) & (ix <= b)
  return f

def price_min_postharvest(data, dates, st):
  dates = dates['harvest'].loc[st]
  d = data[st]
  f = annual_startplus(dates['start'], 9)
  i = yearly_intervals(d.index, f)
  d = d.ix[i['mask']]
  g = d.groupby(i['bucket'])
  return g.min()

def aggregate_states(f):
  def retfun(data, dates, *args):
    means = {}
    for st in dates.index:
      m = f(data, dates, st, *args)
      means[st] = m
    return pd.concat(means,axis=1)
  return retfun
  
price_mean_all = aggregate_states(price_mean)
price_min_postharvest_all = aggregate_states(price_min_postharvest)


def job_fetch_state(dir, comm, s):
  from commloans import fetch
  from pyvirtualdisplay import Display
  
  display = Display(visible=0)
  display.start()
  f = fetch.Fetcher(dir, comm)
  r = f.request_all_counties(s)
  f.close()
  display.stop()
  return r

def calc_prices(crop, how='p', path='./'):
  pcp = ez_read1(os.path.join(path, 'pcp', crop+'.csv'))
  dates = read_dates(os.path.join(path, 'dates', crop+'.txt'))
  # Prices
  if how == 'p':
    d = price_mean_all(pcp, dates, 1) # planting
  elif how == 'h':
    d = price_mean_all(pcp, dates, 0) # harvest
  elif how == 'min':
    d = price_min_postharvest_all(pcp, dates)
  elif how == 'prev':
    d = price_mean_all(pcp, dates, 0).shift(1) # harvest
  return d

def calc_bins(data, n):
  min = data.min()
  diff = data.max() - min
  intervals = (data - min) / (diff / n)
  intervals = intervals.round(0)
  d = pd.DataFrame({'value': data.T.stack(), 'bin': intervals.T.stack()})
  return d
