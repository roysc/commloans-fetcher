import os, sys
from selenium import webdriver
from selenium.webdriver.support.ui import Select
import selenium.common.exceptions as sexc
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from commloans.county_codes import state_names
from commloans._fetcher import Fetcher, debug


url_base = 'https://apps.fsa.usda.gov/sorspub/reports.do?command=displayParameters&reportCatalogName=public&reportName=%s-all-county'



class SummariesFetcher(Fetcher):
  def __init__(self, download_root, what='loan', years=range(2005, 2016)):
    self.url = url_base % what
    super(SummariesFetcher, self).__init__(download_root)
    self.years = years
    
  def get_homepage(self):
    self._dr.get(url_ldp)
    self._wait.until(EC.presence_of_element_located((By.ID, 'cropYear')))

  def request_data(self, state, year):
    debug('request_data:', state_names[state], '(%s)'%state, year)
    
    self.get_homepage()

    elt_state = self._dr.find_element_by_id('state')
    elt_cropyear = self._dr.find_element_by_id('cropYear')

    try:
      Select(elt_state).select_by_value('%02d'%state)
    except sexc.NoSuchElementException as e:
      debug('derp: ', e)
      return False
    Select(elt_cropyear).select_by_visible_text(str(year))
    
    self._dr.execute_script('submitRequest("displayReport")')

    dlpath = os.path.join(self.download_root, str(state))
    self.set_dlpath(dlpath)

    return self.submit_and_export()

  def request_all_years(self, state, cont=True, max_attempts=10):
    res = {}
    for year in self.years:
      attempts = max_attempts
      # while attempts > 0:
      #   try:
      #     if self.request_data(state, year):
      #       res[(state, year)] = True
      #     break
      #   except sexc.WebDriverException as e:
      #     debug(" error:", e)
      #     debug(" attempts left:", attempts)
      #     attempts -= 1
      #     if not attempts:
      #       debug(" max attempts made", state, county)
      #       res[(state, year)] = False
      #       if not cont: raise e
      #     self._dr.refresh()
      self.request_data(state, year)
      
    return res
      
  def request_all_states(self, from_=None):
    res = {}
    codes = sorted(state_names.keys())
    if from_ is not None:
      i = codes.index(from_)
      codes = codes[i:]
    for st in codes:
      r = self.request_all_years(st)
      res.update(r)
    return res
