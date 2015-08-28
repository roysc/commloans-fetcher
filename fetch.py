import os, sys
from selenium import webdriver
from selenium.webdriver.support.ui import Select
import selenium.common.exceptions as sexc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timedelta

import county_codes

def debug(*a):
  print(file=sys.stderr, *a)

acr_url = 'https://apps.fsa.usda.gov/acr/'
download_root = './'

class Fetcher:
    
  def __init__(self, dlpath):
    self._dr = Fetcher._driver(dlpath)
    self.set_wait(10)
    
  def set_wait(self, n):
    self._wait = WebDriverWait(self._dr, n)

  def _driver(dlpath):
    # profile = webdriver.FirefoxProfile()
    # profile.set_preference('browser.download.folderList', 2)
    # profile.set_preference('browser.download.manager.showWhenStarting', False)
    # profile.set_preference('browser.helperApps.neverAsk.saveToDisk', 'text/csv')
    # profile.set_preference('browser.helperApps.neverAsk.openFile', 'text/csv')
    # profile.update_preferences()
    # return webdriver.Firefox(profile)
    options = webdriver.ChromeOptions()
    prefs = {'download.default_directory': dlpath}
    options.add_experimental_option('prefs', prefs)
    return webdriver.Chrome(chrome_options=options)

  def set_dlpath(self, path):
    # self._dr.profile.set_preference('browser.download.dir', path)
    # self._dr.profile.update_preferences()
    pass
    
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
    
  def _get_homepage(self):
    self._dr.get(acr_url)
    self._wait.until(EC.title_contains('Archived'))
    
  def get_states_counties(self):
    self._get_homepage()
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
    debug('request_data:', county_codes.state_names[state], county, year)
    
    self._get_homepage()
    
    elt_state = self._dr.find_element_by_id('state')
    elt_county = self._dr.find_element_by_id('county')

    Select(elt_state).select_by_value(state)
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
    Select(elt_commodity).select_by_visible_text('CORN')

    self._dr.execute_script('submitRequest("displayReport")')

    # Get CSV file
    # dlpath = os.path.join(download_root, str(state), str(county))
    # os.makedirs(dlpath, exist_ok=True)
    # self.set_dlpath(dlpath)
    
    xpath_getcsv = '//*[@title="Comma-Separated Values"]'
    self._wait.until(EC.presence_of_element_located((By.XPATH, xpath_getcsv)))
    self._dr.execute_script('submitRequest("exportToCSV")')

  def request_all_counties(self, state, c=None):
    name = county_codes.state_names[state]
    if c is None:
      counties = county_codes.counties[name]
    elif isinstance(c, str):
      counties = [cty for cty in county_codes.counties[name]
                  if c <= cty]
    debug("counties:", counties)
    for county in counties:
      self.request_all_years(state, county)

  def request_all_years(self, state, county):
    for year in range(2004, 2009):
      attempts = 5
      while attempts > 0:
        try:
          self.request_data(state, county, year)
          break
        except sexc.TimeoutException as e:
          attempts -= 1
          # if not attempts:
          #   debug("max attempts made:", state, county)
          #   raise e
