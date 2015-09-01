import pandas as pd

def read_csv_pop2(fp):

  d = pd.read_csv(fp, header=3)
  # rename first column
  d.rename(columns={d.columns[0]: 'CDP'}, inplace=True)
  d.set_index('CDP', inplace=True)
  # Drop NaNs 
  d.dropna(inplace=True)
  # Drop state-level data
  d.drop(d.index[0], inplace=True)
  # Drop "Estimates Base" and "Census"
  d.drop(d.columns[-2:], axis=1, inplace=True)

  # Remove commas and convert to numeric value
  def nocomma(s):
    r = s.str.replace(',','')
    return r.astype(int)
  d = d.apply(nocomma)

  return d
