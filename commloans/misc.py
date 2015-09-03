import pandas as pd
from datetime import datetime, timedelta

# Get year-offset datetimeindex for month and day
def yearly_dates(ix, startmd, endmd):
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
