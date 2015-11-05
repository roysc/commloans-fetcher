import os, sys, re
import uuid
from selenium import webdriver
from selenium.webdriver.support.ui import Select
import selenium.common.exceptions as sexc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timedelta

from commloans import county_codes

def debug(*a):
  print(file=sys.stderr, *a)

acr_url = 'https://apps.fsa.usda.gov/acr/'
# download_root = './'

COMMODITY_OPTIONS = {
  'BARLEY',
  'CANOLA',
  'CORN',
  'CRAMBE',
  'DRY PEAS',
  'FLAXSEED',
  'GRAIN SORGHUM',
  'MUSTARD SEED',
  'OATS',
  'RAPESEED',
  'RICE',
  'SAFFLOWER',
  'SOYBEANS',
  'SUNFLOWER OIL',
  'SUNFLOWER SEED',
  'WHEAT',
}

class Fetcher:
    
  def __init__(self, download_root, commodity, years=range(2004, 2015)):
    self.download_root = os.path.realpath(download_root)
    if commodity not in COMMODITY_OPTIONS:
      raise ValueError('invalid commodity')
    self.commodity = commodity
    self.years = years
    linkname = str(uuid.uuid4())
    self._dltarget = os.path.join(download_root, linkname)
    self._dr = Fetcher._make_driver(self._dltarget)
    self.set_wait(1.5)
    
  def set_wait(self, n):
    self._wait = WebDriverWait(self._dr, n)

  def _make_driver(dlto):
    # profile = webdriver.FirefoxProfile()
    # profile.set_preference('browser.download.folderList', 2)
    # profile.set_preference('browser.download.manager.showWhenStarting', False)
    # profile.set_preference('browser.helperApps.neverAsk.saveToDisk', 'text/csv')
    # profile.set_preference('browser.helperApps.neverAsk.openFile', 'text/csv')
    # profile.set_preference('browser.download.dir', dlto)
    # profile.update_preferences()
    # return webdriver.Firefox(profile)
    options = webdriver.ChromeOptions()
    prefs = {'download.default_directory': dlto}
    options.add_experimental_option('prefs', prefs)
    return webdriver.Chrome(chrome_options=options)

  def close(self):
    self._dr.close()
  
  def set_dlpath(self, path):
    # Hack: download dir is symlink, just change its target
    if os.path.exists(self._dltarget):
      os.remove(self._dltarget)
    os.symlink(path, self._dltarget, target_is_directory=True)
    
  # Returns true if page was refreshed
  def _wait_for_counties(self, elt_county):
    reloaded = False
    try:
      self._wait.until(EC.staleness_of(elt_county))
    except sexc.TimeoutException as e:
      # if it timed out, maybe needs to "refresh"
      self._dr.execute_script('submitRequest("displayParameters")')
      self._wait.until(EC.staleness_of(elt_county))
      reloaded = True
    self._wait.until(EC.presence_of_element_located((By.ID, 'county')))
    return reloaded
    
  def get_homepage(self):
    self._dr.get(acr_url)
    self._wait.until(EC.title_contains('Archived'))
    
  def get_states_counties(self):
    self.get_homepage()
    # present = EC.presence_of_element_located(By.ID, 'county')

    elt_state = self._dr.find_element_by_id('state')

    by_value, by_name = {}, {}
    for opt in Select(elt_state).options:
      by_value[opt.get_attribute('value')] = opt.text
      by_name[opt.text] = []

    elt_county = self._dr.find_element_by_id('county')
    for key, name in by_value.items():
      Select(elt_state).select_by_value(key)
      # Have to wait until the damn county list updates
      if self._wait_for_counties(elt_county):
        elt_state = self._dr.find_element_by_id('state')

      elt_county = self._dr.find_element_by_id('county')
      for opt in Select(elt_county).options:
        # print(name, opt.text)
        by_name[name].append(opt.get_attribute('value'))

    return by_value, by_name

  def request_data(self, state, county, year):
    debug('request_data:', county_codes.state_names[state], '(%s)'%state, county, year)
    
    self.get_homepage()
    
    elt_state = self._dr.find_element_by_id('state')
    elt_county = self._dr.find_element_by_id('county')

    Select(elt_state).select_by_value('%02d'%state)
    self._wait_for_counties(elt_county)

    elt_county = self._dr.find_element_by_id('county')
    Select(elt_county).select_by_value(county)

    elt_cropyear = self._dr.find_element_by_id('cropYear')
    elt_begindate = self._dr.find_element_by_id('beginningDate')
    elt_enddate = self._dr.find_element_by_id('endingDate')

    # Set begin and end: e.g. '01/01/2005', '01/01/2006'
    if year < 2004 or year > 2015:
      raise ValueError("year must be between [2004, 2015]", year)

    begindate = datetime(year, 1, 1)
    enddate = datetime(year+1, 1, 1)
    cropyear = year - 1
    if year == 2004: cropyear = 2004
    timefmt = '%m/%d/%Y'

    elt_begindate.send_keys(begindate.strftime(timefmt))
    elt_enddate.send_keys(enddate.strftime(timefmt))
    Select(elt_cropyear).select_by_visible_text(str(cropyear))

    elt_commodity = self._dr.find_element_by_id('commodity')
    Select(elt_commodity).select_by_visible_text(self.commodity)

    # xpath_run = '//*[@title="Run Report"]'
    # btn_run = self._dr.find_element_by_xpath(xpath_run)
    # btn_run.click()
    self._dr.execute_script('submitRequest("displayReport")')
    
    # Get CSV file
    dlpath = os.path.join(self.download_root, str(state), str(county))
    os.makedirs(dlpath, exist_ok=True)
    self.set_dlpath(dlpath)
    
    xpath_getcsv = '//*[@title="Comma-Separated Values"]'
    try:
      self._wait.until(EC.presence_of_element_located((By.XPATH, xpath_getcsv)))

    # Timed out. Maybe no results?
    except sexc.TimeoutException as e:
      try:
        elt_feedback = self._dr.find_element_by_xpath('//*[@id="report-feedback"]/p')
        if re.search('No results found', elt_feedback.text):
          debug(' ...no results')
          return False
      except sexc.NoSuchElementException:
        raise e
  
    self._dr.execute_script('submitRequest("exportToCSV")')
    
    return True
  
  def request_all_years(self, state, county, cont=True):
    res = {}
    for year in self.years:
      attempts = 10
      while attempts > 0:
        try:
          if self.request_data(state, county, year):
            res[(state, county, year)] = True
          break
        except sexc.WebDriverException as e:
          debug(" error:", e)
          debug(" attempts left:", attempts)
          attempts -= 1
          if not attempts:
            debug(" max attempts made", state, county)
            res[(state, county, year)] = False
            if not cont: raise e
          self._dr.refresh()

    return res
    
  def request_all_counties(self, state, counties=None, from_=None):
    name = county_codes.state_names[state]
    if counties is None:
      counties = county_codes.counties[state]
    if from_ is not None:
      counties = [cty for cty in counties if from_ <= int(cty)]
    # debug("counties:", counties)
    res = {}
    for county in counties:
      r = self.request_all_years(state, county)
      res.update(r)
    return res

class FetcherFactory:
  def __init__(self, download_root, commodity, years):
    self.dlroot = download_root
    self.commodity = commodity
    self.years = years

  def create(self):
    return Fetcher(self.dlroot, self.commodity, self.years)
