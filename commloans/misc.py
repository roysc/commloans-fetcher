import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
  
import matplotlib.pyplot as plt

import commloans.county_codes as cc
from commloans import reg

statecodes = sorted(cc.state_names.keys())
_prices = ['minp', 'lastp', 'harvestp', 'plantp']
_crops = ['corn','oats','sorghum','soy','wheat']


def ez_read2(f,dates=0):
  "Read csv with 2 header rows"
  d = pd.read_csv(f, header=[0,1], parse_dates=dates, index_col=0)
  levs = d.columns.levels
  ilevs = [[int(s) for s in l] for l in levs]
  d.columns.set_levels(ilevs,inplace=True)
  return d

def ez_read3(f,dates=True):
  "Read csv with 3 header rows"
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

def calc_prices(crop, how='plant', path='./', data=None):
  print('calc_prices:', crop, how)
  if data is not None:
    pcp = data
  else:
    pcp = ez_read2(os.path.join(path, 'pcp', crop+'.csv'))
  dates = read_dates(os.path.join(path, 'dates', crop+'.txt'))
  # Prices
  if how == 'plant':
    d = price_mean_all(pcp, dates, 1) # planting
  elif how == 'harvest':
    d = price_mean_all(pcp, dates, 0) # harvest
  elif how == 'min':
    d = price_min_postharvest_all(pcp, dates)
  elif how == 'last':
    d = price_mean_all(pcp, dates, 0).shift(1) # harvest
  return d

def cleanup(dir):
  ds = {}
  for file in os.listdir(dir):
    # reshape
    file = os.path.join(dir, file)
    d = pd.read_csv(file, index_col=[0,1])
    crop = d.Commodity.iloc[0]
    d = d.reset_index().set_index('state county Year'.split())
    # d = d['Value'].sort_index()
    d = d['Value'].unstack().T
    d.index.name = 'year'
    ds[crop.lower()] = d
  return pd.concat(ds,axis=1)

def getall_prices(crop):
  ds = {}
  for k in 'plant harvest min last'.split():
    p = calc_prices(crop, k)
    p = p.T.stack()
    p.index.names = 'state county year'.split()
    p = p.reset_index().set_index('year state county'.split())
    p.sort_index(inplace=1)
    ds[k+'p'] = p
  return pd.concat(ds, axis=1)


def plot_rdgraph(x, y, nbins):
  """Plot RD graph.
  x: X-axis variable, e.g. price difference (pcp - loanrate)
  y: outcome Y-axis variable, e.g. next year's area planted
  """

  # Concatenate the data for X and outcome variable
  # this combines them horizonally, so you end up with 2 columns, named X and Y
  # The index needs to be consistent so the data can be grouped
  d = pd.concat({'X':x, 'Y':y}, axis=1, join_axes=[x.index])
  # Get an array of N evenly distributed X-axis points for bin boundaries
  # with one bin having a boundary at exactly 0
  mn, mx = x.min(), x.max()
  binsize = (mx - mn)/(nbins-2)
  shift = (np.floor(mn/binsize) - mn/binsize)
  mn += shift * binsize
  mx += (1 + shift) * binsize
  bins = np.linspace(mn, mx, nbins)
  binlabels = np.linspace(mn + binsize/2, mx + binsize/2, nbins)
  # Bin indices ("ix" is short for index) from "cutting" the data according to the bins
  # labels will be how they are identified. To make it clear, each bin is labeled with
  # its lower bound
  bix = pd.cut(x, bins, labels=binlabels[:-1])
  # Group data (combined X & Y) according to bin indices
  g = d.groupby(bix)
  # "Agg"regate by taking median of X values, and mean of the outcome
  midmean = g.agg({'X':'median','Y':'mean'})

  print("creating graph.", 'bin size:', binsize)
  # Create graph...
  fig = plt.figure()
  plt.yscale('log')             # logarithmic y-axis
  
  # Scatter plot original area data
  plt.scatter(bix, d['Y'], color='blue', marker='+')
  # Overlay with mean of data
  plt.scatter(midmean.index, midmean['Y'], color='red')
  # Vertical line at 0
  plt.axvline(x=0, color='black', linestyle='--')
  
  # TODO: Train OLS on median -> mean values, plot regression line...
  # import statsmodels.formula.api as smf
  # lm = smf.ols()
  
  return fig, binsize

# Convenience
def ez_save_plot(pr, lr, y, crop, kind='all', yname=('area','Area planted (ac.)'), nbins=40):
  "Create and save plot with a reasonable name"
  # Pass "all" to do all price types at once
  if kind == 'all':
    for k in 'plantp harvestp minp lastp'.split():
      ez_save_plot(pr, lr, y, crop, k, yname, nbins)
    return

  yc = y.get(crop)
  if yc is not None: y = yc
  fig, binsize = plot_rdgraph((pr[kind]-lr)[crop], y, nbins)
  fig.suptitle('%s (%s bins, width=%.4f)'%(crop.capitalize(), nbins, binsize))
  plt.xlabel('PCP - Loanrate ($)')
  plt.ylabel(yname[1])
  
  # String substitution: %s is replaced with a string or int
  path = '%s-%s-%s-%s.png'%(crop,kind,yname[0],nbins)
  print('saving to', path)
  plt.savefig(path)
  plt.close()

def plot_prices():
  ds={}
  for f in os.listdir():
    d=pd.read_csv(f,index_col=0,parse_dates=0,header=[0,1])
    ds[f[:-4]]=d
  means={}
  for k,d in ds.items():
    means[k]=d.mean(axis=0)
  p=pd.concat(means,axis=1)
  p.plot()

def _make_table_desc(dataframes, nlr, price, pricetitle):
  # import reg

  text_beg = r"""
\begin{threeparttable}
\caption{Descriptive statistics: %s}
\label{des}
\begin{tabular}{l cccc|cccc}
\hline\hline
& \multicolumn{4}{c}{2004---2008} & \multicolumn{4}{c}{2004---2014} \\
\hline  
& diff  $<$ 0 & diff $\geq$ 0 & diff nlr $<$ 0 & diff nlr $\geq$ 0
& diff  $<$ 0 & diff $\geq$ 0 & diff nlr $<$ 0 & diff nlr $\geq$ 0\\
\hline
"""% pricetitle
  
  text_end = r"""\end{tabular}
\begin{tablenotes}[flushleft]\footnotesize 
\item[1] Mean value and Standard deviation in parenthesis. 
\end{tablenotes}
\end{threeparttable}
"""

  variables = [
    ('area_next', "Acres planted"),
    ('prod_next', "Production"),
  ]

  def make_part(data):
    # gcrops = data.groupby(level='crop',axis=1)
    gyear = data[price].groupby(level='year',axis=0)
    def fnlr(d): return d - nlr.loc[d.name]
    nlrdiff = gyear.transform(fnlr)

    sections = {}
    for var, vartitle in variables:
      res = pd.DataFrame(columns=_crops)
      for crop in _crops:
        nums = []
        # County LR
        lt = data[price, crop] < data['loanrate', crop]
        gt = data[price, crop] >= data['loanrate', crop]
        
        # National LR is much more complicated, data is shaped differently
        # group by year
        nlt = nlrdiff[crop] < 0
        ngt = nlrdiff[crop] >= 0
        
        for ix in (lt, gt, nlt, ngt):
          nums.append((data.loc[ix, (var, crop)].mean(),
                       data.loc[ix, (var, crop)].std()))
        res[crop] = nums
      sections[var] = res
      
    return sections

  # All sections for all data parts
  sections_list = [make_part(d) for d in dataframes]

  # Now make the text
  text_mid = ""
  for var, vartitle in variables:
    text = r"\emph{%s} \\" % vartitle + '\n'

    res = pd.concat(secs[var] for secs in sections_list)
    for crop, nums in res.items():
      row = [crop.capitalize()] + [r'\specialcell{%.4f\\(%.4f)}' % n for n in nums]
      text += ' & '.join(row) + r' \\'+'\n'
    text += r'\hline' + '\n'

    text_mid += text
    
  return text_beg + text_mid + text_end

fancy_prices = [
  ('minp', "Min price"),
  # ('lastp', "Last price"),
  ('plantp', "Price during planting"),
  ('harvestp', "Price during harvest"),
]

def make_desc_table_file(data, nlr, file):
  tpl = r"""\documentclass{article}
\usepackage{threeparttablex}
\newcommand{\specialcell}[2][c]{%%
  \begin{tabular}[#1]{@{}c@{}}#2\end{tabular}}
  
\begin{document}
%s
\end{document}
"""
  s = []
  for p, pt in fancy_prices:
    s.append(_make_table_desc(data, nlr, p, pt))
  with open(file, 'w') as f:
    f.write(tpl % '\n'.join(s))


def _latex_level_slope(d):
  if d.isnull().any():
    return r'$\cdot$'
  pval = d['p']
  stars = ''
  if pval < 0.1: stars = r'^{\dag}'
  if pval < 0.05: stars = r'^{\ast}'
  if pval < 0.01: stars = r'^{\ast\ast}'
  cell = r'$\underset{(%.4f)}{%.4f%s}$' % (d['std'], d['val'], stars)
  return cell
    
    
def _latex_coeff_table(data, crop):
  
  tab, aic = reg.make_coeff_table(data, crop)
  
  text_beg = r"""
\newpage
\begin{threeparttable}
\caption{Choice of models: %s}
\label{choose-%s}
\scalebox{0.93}{\parbox{\linewidth}{
\begin{tabular}{l c c  c | c c  c}
\hline\hline
& \multicolumn{3}{c}{With no covariates} & \multicolumn{3}{c}{With covariates} \\
\cline{2-4} \cline{5-7} 
 & Model 1 & Model 2 & Model 3& Model 1 & Model 2& Model 3\\
""" % (crop, crop)
  
  text_end = r"""
\end{tabular}}}
\begin{tablenotes}[flushleft]\footnotesize 
\item[1] $\dag$: $p < 0.1$; $\ast$: $p < 0.05$; $\ast\ast$: $p < 0.01$.
\end{tablenotes}
\end{threeparttable}
"""

  lines = []
  for p, pt in fancy_prices:
    lines.append( r"\emph{%s} \\" % pt)
    for ls in ['level', 'slope']: # pdata.index.get_level_values(0)
      l = []
      pdata = tab.loc[p, ls]
      for col in pdata.columns:
        d = pdata[col]
        l.append(_latex_level_slope(d))
      row = ["Treatment estimate (%s)" % ls.capitalize()] + l
      lines.append( ' & '.join(row) + r'\\')
    lines.append(' & '.join(["AIC"] + list('$%.2f$'%x for x in aic.loc[p])) + r'\\')
    lines.append(r'\hline')
    
  return text_beg + '\n'.join(lines) + text_end


def latex_coeff_table_file(file, data):
  tpl = r"""\documentclass[a4paper, 12pt]{article}
\usepackage{threeparttablex}
\usepackage{amsmath}
\usepackage{graphicx}

\begin{document}
%s
\end{document}
"""  
  s = []
  for crop in _crops:
    s.append(_latex_coeff_table(data, crop))
  with open(file, 'w') as f:
    f.write(tpl % '\n'.join(s))


def _latex_across_crops(dvc, price, pricetitle):
  text_beg = r"""
\begin{threeparttable}
\caption{Effects across crops: %s}
\label{across-%s}
\begin{tabular}{l |l c c c c c  }
\hline\hline
""" % (pricetitle, price)

  text_end = r"""
\end{tabular}
\begin{tablenotes}[flushleft]\footnotesize 
\item[1] $\dag$: $p < 0.1$; $\ast$: $p < 0.05$; $\ast\ast$: $p < 0.01$.
\end{tablenotes}
\end{threeparttable}
\newpage
"""

  tab = reg.make_across_crops(dvc, price)
  
  lines = []
  lines.append('& & ' + ' & '.join(_crops) + r'\\ \hline')
  for crop in _crops:
    for pre, ls in [(r'\multirow{2}{*}{%s}' % crop, 'level'),
                    ('', 'slope')]:
      part = tab.loc[crop, ls]
      row = [_latex_level_slope(part[c]) for c in part.columns]
      row = [pre, ls] + row
      lines.append(' & '.join(row) + r'\\')
    lines.append(r'\hline')

  return text_beg + '\n'.join(lines) + text_end

def latex_across_crops_file(file, dvc):
  tpl = r"""\documentclass[a4paper, 12pt]{article}
\usepackage{threeparttablex}
\usepackage{amsmath}
\usepackage{graphicx}
\usepackage{multirow}

\begin{document}
%s
\end{document}
"""  
  s = []
  for p,pt in fancy_prices:
    s.append(_latex_across_crops(dvc, p, pt))
  with open(file, 'w') as f:
    f.write(tpl % ('\\newpage\n'.join(s)))
