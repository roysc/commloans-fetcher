Examples
----

Go to data directory:
```sh
$ cd data/
$ ls
01/ 02/ 03/
```

Run (i)python: `PYTHONPATH=~/Dropbox/lilz/ll2yp ipython`

```python
from loanrates import reader

r = reader.Reader('.')
d = r.process_all_counties('01')
# d now contains multi-indexed data from all counties in Alabama

# select data for 2007
d07 = d.ix[d.index.year == 07]

# plot it
from matplotlib import pyplot as plt
# legend is too big
d07.plot(legend=None)
plt.show()

```
