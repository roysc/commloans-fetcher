import numpy as np
import pandas as pd
import statsmodels.formula.api as smf

from commloans.misc import _crops, _prices

# _int_terms = ['D * diffp', 'diffp**2']

def regression(dall, crop, price, term, log=True):

  d = dall[crop]
  d = pd.concat([d, dall['0']], axis=1)
    
  d['diffp'] = d[price] - d.loanrate
  d['D'] = (d.diffp < 0)
  d.agchar /= 100
  d['pop'] = np.log(d['pop'])
  if log:
    d.area_next = np.log(d.area_next)
    d.area = np.log(d.area)

  terms = ['D', 'diffp', 'area', 'pop', 'agchar']
  terms.append(term)
  reg = 'area_next ~ ' + ' + '.join(terms)
  # reg = "area_next ~ D + diffp + area + pop + agchar + " + term

  print('running:', crop, price, term, log)
  ls = smf.ols(reg, d)
  res = ls.fit()
  return res


def main(data, out):
  results, titles = [], []
  for crop in _crops:
    for price in _prices:
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


if __name__ == '__main__':
  import sys
  fdata, fout = sys.argv.get[1:3]
  data = pd.read_csv(fdata, index_col=[0,1,2], header=[0,1])
  out = open(fout, 'w')
  main(data, out)

