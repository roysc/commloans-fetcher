Examples
----

Go to data directory:
```sh
$ cd data/
$ ls
01/ 02/ 03/
```

Run (i)python: `PYTHONPATH=~/Dropbox/commloans ipython`

```python
from loanrates import fetch, reader

# fetch corn data
f = fetch.LoanRateFetcher('.', 'CORN')
# get all data for state with code 1 (AL)
f.request_all_counties(1)

r = reader.LoanRateReader('.')
# read data by county and state code
d = r.process_all_files(1, 1)
# d now contains multi-indexed data from Autauga, AL

# or just use state code:
d = r.process_all_counties(1)
# or do all states at once:
d = r.process_all_states()

# select data for 2007
d07 = d.ix[d.index.year == 07]

# plot it
from matplotlib import pyplot as plt
# legend is too big
d07.plot(legend=None)
plt.show()

```
