from events_hitparade_co.scrapers.scraper_interface import WebScraper
import os
from selenium import webdriver
class FirefoxWebScraper(WebScraper):
    """
    Firefox version of webscraper
    """
    def __init__(self, **kwargs):
        """
        FirefoxWebScrpaer Constructor
        :param headless: True if you want your browser to run headless and false if you want to see the browser running
        :param start_url: str start url
        :param timeout: 5 if you want
        :param id: int id the web scraper should use.  If it is -1, generate a new id from MessagingQueue
        """
        # headless=False, start_url=None, cache_manager=None, timeout=WebScraper.DEFAULT_WEB_SCRAPER_TIMEOUT, id=-1
        super().__init__(**kwargs)

    def create_driver(self):
        """
        Creates the browser specific driver in this instance Firefox
        :return:
        """
        self.driver = webdriver.Firefox()

    def set_headless(self):
        """
        sets the scraper to be headless
        override for each browser
        """
        os.environ['MOZ_HEADLESS'] = '1'

    class Factory:
        """
        Factory class used by the WebScraperFactory
        """
        def create(self, **kwargs): return FirefoxWebScraper(**kwargs)