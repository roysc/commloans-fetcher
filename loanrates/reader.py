import pandas as pd
import decimal

column_names = [
  'commodity',
  'date',
  'loan_rate',
  'pcp_30day',
  'pcp_alternative',
  'effective_pcp',
  'effective_ldp',
  'effective_acre_ldp',
]

def read_csv(fp):
  d = pd.read_csv(fp,
                  skiprows=4,
                  parse_dates=['Effective Date'])
  d.columns = column_names
  # Commodity always == Corn
  d = d.drop('commodity', axis=1)

  for col in ('loan_rate', 'effective_pcp', 'effective_ldp'):
    d[col] = d[col].str.strip('$ ').astype(float)

  d.set_index('date', inplace=True)
  return d
