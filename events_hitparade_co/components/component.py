import traceback
from events_hitparade_co.bots.bot import HitParadeBot
from events_hitparade_co.registration.registration import RegisterLeafClasses
from abc import abstractmethod
from selenium.webdriver.common.keys import Keys
class ScraperComponent:

    __metaclass__ = RegisterLeafClasses
    """
    Basic Scraper Component Class.
    Basic things that inputing data and scraping data will need.
    """
    def __init__(self,**kwargs):
        """
        Constructor for all ScraperComponents
        :param **kwargs dict using the following data.
                driver:  WebScraper driver default None
                web_driver: WebScraper  default None
                scraper_url: str url to open default None
                open_url: bool True to open upon startup and False it waits.
                command:  str command this component responds to and runs.
                default_parser: HitParadeParser defaults to BeautifulSoupParser
                retry_count: int number of times to retry anything default HitParadeBot.DEFAULT_RETRY
        """
        self.__dict__ = dict(list(kwargs.items()) + list(self.__dict__.items()))
        self.driver = kwargs.get('driver', None)
        self.scraper_url =  self.state_storage_get_prop('scraper_url')  #kwargs.get('scraper_url', None)
        self.retry_count = kwargs.get('retry_count', HitParadeBot.DEFAULT_RETRY)
        self.command = kwargs.get('command', None)
        self.open_url = self.state_storage_get_prop('data_selectors').get('open_url', True) #kwargs.get('data_selectors', {}).get('open_url', True)
        self.cache_manager = kwargs.get('cache_manager', None)
        parser_kwargs = {'driver' : self.driver}
        self.parser_kwargs = kwargs
        try:
            self.default_parser = kwargs.get('default_parser', 'BeautifulSoupParser')
            if self.default_parser is None:
                self.default_parser = self.cache_manager.cache_output_component_func(kwargs.get('default_parser', 'BeautifulSoupParser'), **kwargs)
        except:
            print('exception making parser')
            traceback.print_exc()
        #scraper component web driver
        self.use_once = kwargs.get('use_once', False)
        self.use_until_failure = kwargs.get('use_until_failure', False)
        self.web_driver = kwargs.get('web_driver', None)
        print('ScrapeComponent init open()')
        self.force_refresh = kwargs.get('force_refresh', False)
        # self.open()

    @abstractmethod
    def exec(self,**kwargs):
        """
        Method that is called by the WebScraper
        :param kwargs: dict of data
        :return:
        """
        pass

    @abstractmethod
    def perform_action(self, **kwargs):
        """
        Action for ScraperComponent
        :param kwargs: dict of data
        :return:
        """
        pass

    def respond(self, val):
        """
        respond to main thread
        :param val: dict to send bac
        :return:
        """
        if not self.web_driver is None:
            self.web_driver.respond(obj=val, command=self.command)

    def quit(self):
        """
        ScraperComponet Quit
        :return:
        """
        try:
            self.web_driver.quit()
            return True
        except:
            return False

    def get_driver(self):
        """
        returns the driver
        :return: WebScraper driver
        """
        return self.driver

    def get_scraper_url(self):
        """
        Returns the str scraper url
        :return: str scraper_url
        """
        return self.scraper_url

    def get_retry_count(self):
        """
        returns retry count
        :return: int retry_count
        """
        return self.retry_count

    def get_open_url(self):
        """
        returns the open url
        :return: bool True or False to open upon start.
        """
        return self.open_url


    def open(self):
        """
        Opens the open url if necessary.
        :return: bool True if opened and False if not opened.
        """
        try:
            print('open( open_url=%s, force_refresh=%s, scraper_url=%s)'%( str(self.open_url), str(self.force_refresh), str(self.scraper_url)))
            if ((not self.open_url) or self.force_refresh) and not self.driver is None and not self.scraper_url is None:
                print( 'refresh %s ' % self.scraper_url )
                self.driver = self.web_driver.driver
                self.driver.get(self.scraper_url)
                self.driver.implicitly_wait(15)
                # self.driver.refresh()
                # self.driver.find_element_by_css_selector('body').send_keys(Keys.F5)
                self.parser_kwargs['driver'] = self.driver
                self.parser_kwargs['web_driver'] = self.web_driver
                try:
                    self.default_parser =  self.parser_kwargs.get('default_parser', None)
                    if self.default_parser is None:
                        self.default_parser = self.cache_manager.cache_output_component_func( self.parser_kwargs.get('default_parser', 'BeautifulSoupParser'), **self.parser_kwargs)
                    self.default_parser.driver = self.web_driver.driver
                    self.default_parser.reload_content()
                except:
                    print('exception making parser')
                    traceback.print_exc()
                self.open_url = True
                return True
        except:
            traceback.print_exc()
        return False
