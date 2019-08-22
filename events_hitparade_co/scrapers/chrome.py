from events_hitparade_co.scrapers.scraper_interface import WebScraper
from selenium import webdriver
class ChromeWebScraper(WebScraper):
    """
    Chrome version of webscraper
    """
    def __init__(self,**kwargs):
        """
        Chrome Constructor
        :param headless: True if you want your browser to run headless and false if you want to see the browser running
        :param start_url: str start url
        :param timeout: 5 if you want
        :param id: int id the web scraper should use.  If it is -1, generate a new id from MessagingQueue
        """
        # headless=True, start_url=None, cache_manager=None, timeout=WebScraper.DEFAULT_WEB_SCRAPER_TIMEOUT, id=-1
        super().__init__(**kwargs)
        self.chrome_binary = kwargs.get('chrome_binary', kwargs.get('global_variables', {}).get('chrome_binary', '/usr/bin/chromedriver'))
        self.google_chrome_binary = kwargs.get('google_chrome_binary', kwargs.get('global_variables', {}).get('google_chrome_binary', '/usr/bin/google-chrome'))
        print('chrome scraper running at %s ' % str(self.id))


    def start_virtual_driver(self):
        print('starting virtual display')
        from pyvirtualdisplay import Display 
        self.display = Display(visible=0, size=(1024, 768)) 
        self.display.start()
 
    def create_driver(self):
        """
        Creates the browser specific driver in this instance Chrome
        :return:
        """
        #self.start_virtual_driver()
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('window-size=1200x600')
        self.options.add_argument('--start-maximized')
        self.options.add_argument('--disable-popup-blocking') 
        self.options.add_argument('--no-sandbox')
        #self.options.add_argument("--disable-setuid-sandbox")
        #user_data = '--user-data-dir='+self.google_chrome_binary
        #self.options.add_argument(user_data)
        #self.options.binary_location = self.google_chrome_binary
        print('driver_path=%s' % self.chrome_binary)
        print('chrome binary=%s' % self.google_chrome_binary)
        self.driver = webdriver.Chrome(chrome_options=self.options, service_args=['--verbose'])
        # -- linux setup self.driver = webdriver.Chrome(self.chrome_binary, chrome_options=self.options,service_args=['--verbose', '--log-path=/tmp/chromedriver.log'])
        self.driver.switch_to.window(self.driver.window_handles[-1])
        print('number windows is %s ' % str(len(self.driver.window_handles[-1])))


    def release_driver(self):
        try:
            self.driver.quit()
        except:
            pass

    def set_headless(self):
        """
        sets the scraper to be headless
        override for each browser
        """
        self.options.add_argument('headless')
        self.options.add_argument('-headless')

    class Factory:
        """
        Factory class used by the WebScraperFactory
        """
        def create(self, **kwargs): return ChromeWebScraper(**kwargs)
