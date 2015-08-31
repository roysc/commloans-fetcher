import pandas as pd

def read_csv(fp):

  d = pd.read_csv(fp, header=3)
  # rename first column
  d.rename(columns={d.columns[0]: 'CDP'}, inplace=True)
  # Drop NaNs 
  d.dropna()
  # Drop state-level data
  d.drop(d.index[0])
