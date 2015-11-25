import os, sys, re
import uuid
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
import selenium.common.exceptions as sexc
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

def debug(*a):
  print(file=sys.stderr, *a)
    
class Fetcher:
  def __init__(self, download_root):
    self.download_root = os.path.realpath(download_root)
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
    os.makedirs(path, exist_ok=True)

  # def get_homepage(self):
  #   self._dr.get(acr_url)
  #   self._wait.until(EC.title_contains('Archived'))

  def submit_and_export(self):

    self._dr.execute_script('submitRequest("displayReport")')
    
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
    
