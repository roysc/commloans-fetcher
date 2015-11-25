import numpy as np
import pandas as pd
import statsmodels.formula.api as smf

from commloans.misc import CROPS, PRICES

# _int_terms = ['D * diffp', 'diffp**2']

# def regression(dall, crop, price, term, log=True):

#   d = dall[crop]
#   d = pd.concat([d, dall['0']], axis=1)
    
#   d['diffp'] = d[price] - d.loanrate
#   d['D'] = (d.diffp < 0)
#   d.agchar /= 100
#   d['pop'] = np.log(d['pop'])
#   if log:
#     d.area_next = np.log(d.area_next)
#     d.area = np.log(d.area)

#   terms = ['D', 'diffp', 'area', 'pop', 'agchar']
#   terms.append(term)
#   reg = 'area_next ~ ' + ' + '.join(terms)
#   # reg = "area_next ~ D + diffp + area + pop + agchar + " + term

#   print('running:', crop, price, term, log)
#   ls = smf.ols(reg, d)
#   res = ls.fit()
#   return res


def _regr(d, cov, model):

  d['agchar'] /= 100
  
  if model >= 2:
    d['diffp2'] = d['diffp']**2
  if model >= 3:
    d['diffp3'] = d['diffp']**3
    
  models = {
    1: "diff_ln_area ~ D + diffp + D * diffp",
    2: "diff_ln_area ~ D + diffp + diffp2  + D * diffp + D * diffp2",
    3: "diff_ln_area ~ D + diffp + diffp2 + diffp3 + D * diffp + D * diffp2 + D * diffp3",
  }
  reg = models[model]
  if cov:
    reg += " + ln_area + ln_pop + agchar"

  print(reg)
  ls = smf.ols(reg, d)
  res = ls.fit()
  return res

def regression(dall, crop, price, cov, model):
  d = dall[crop]
  d = pd.concat([d, dall['0']], axis=1)
  d['diffp'] = d[price] - d.loanrate
  d['D'] = (d.diffp < 0)
  d['ln_area'] = np.log(d['area'])
  d['ln_area_next'] = np.log(d['area_next'])
  d['ln_pop'] = np.log(d['pop'])
  d['diff_ln_area'] = d['ln_area_next'] - d['ln_area']

  print('running:', crop, price, cov, model)
  return _regr(d, cov, model)


def get_level_slope(res):
  li, si = 'D[T.True]', 'D[T.True]:diffp'
  return ((res.params[li], res.bse[li], res.pvalues[li]),
          (res.params[si], res.bse[si], res.pvalues[si]))


def make_across_crops(dvc, price):
  from itertools import product
  crop_pairs = product(CROPS, CROPS)

  def bcast(n, what):
    mi = pd.MultiIndex.from_product([n, what.columns])
    dvc[mi] = what

  bcast('diffp', dvc[price] - dvc['loanrate'])
  bcast('D', (dvc['diffp'] < 0))
  bcast('ln_area', np.log(dvc['area']))
  bcast('ln_area_next', np.log(dvc['area_next']))
  bcast('ln_pop', np.log(dvc['pop']))
  bcast('diff_ln_area', dvc['ln_area_next'] - dvc['ln_area'])

  dcv = dvc.swaplevel(0,1,axis=1).sort_index(axis=1)

  rowix = pd.MultiIndex.from_product([CROPS,
                                      ['level','slope'],
                                      ['val','std','p']])
  table = pd.DataFrame(index=rowix, columns=CROPS)
  
  for crop1, crop2 in crop_pairs:
    if crop1 == crop2: continue
    d = dcv[crop2].copy()
    d['diff_ln_area'] = dcv[crop1, 'diff_ln_area']
    d['ln_pop'] = dcv['0', 'ln_pop']
    d['agchar'] = dcv['0', 'agchar']
    d['ln_area'] = dcv[crop1, 'ln_area']
    
    res = _regr(d, 1, 3)
    l, s = get_level_slope(res)
    table.loc[(crop1, 'level'), crop2] = l
    table.loc[(crop1, 'slope'), crop2] = s

  return table
    
def make_coeff_table(dall, crop):
  do_prices = ['minp', 'plantp', 'harvestp']
  
  colix = pd.MultiIndex.from_product([[False,True],[1,2,3]])
  rowix = pd.MultiIndex.from_product([do_prices,
                                      ['level','slope'],
                                      ['val','std','p']])
  table = pd.DataFrame(index=rowix, columns=colix)
  aic = pd.DataFrame(index=do_prices, columns=colix)
  
  for c, m in colix:
    for p in do_prices:
      res = regression(dall, crop, p, c, m)
      l, s = get_level_slope(res)
      table.loc[(p, 'level'), (c,m)] = l
      table.loc[(p, 'slope'), (c,m)] = s
      aic.loc[p, (c,m)] = res.aic

  return table, aic


def main(data, out):
  results, titles = [], []
  for crop in CROPS:
    for price in PRICES:
      for term, kind in [('D * diffp', "with interaction"),
                         ('diffp**2', "quadratic, without interaction")]:
        for log in [True, False]:
          r = regression(data, crop, price, term, log)
          results.append(r.summary())
          t = "%s (%s), %s; %s" % (crop.capitalize(), price, kind,
                                   "logarithmic" if log else "not logarithmic")
          titles.append(t)

  text = r"""
  \documentclass{article}
  \usepackage{booktabs}
  
  \begin{document}
  %s
  \end{document}
  """
  insert = []
  for r, t in zip(results, titles):
    s = r.as_latex()
    ins = '%s:\n' % t + s.replace('_', r'\_')
    insert.append(ins)

  out.write(text % '\n'.join(insert))


