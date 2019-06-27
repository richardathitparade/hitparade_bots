from events_hitparade_co.components.component import ScraperComponent
from events_hitparade_co.components.action import ScraperAction
from events_hitparade_co.components.input import ScraperInputFields
import traceback
class ScraperLogin(ScraperComponent):
    """
    Python class that will log into an interface
    """
    def __init__(self, **kwargs ):
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
                       ** selectors: Selectors for input fields
                       ** login_page_selectors: Selectors to deem if we are on the specific login page
                       ** retry_count: number of times to retry inputting fields or clicking a button
        """
        super().__init__(  **kwargs )
        self.login_page_selectors = kwargs.get('login_page_selectors', None)
        self.selectors = kwargs.get('selectors', None)
        self.login_button_selector = kwargs.get('login_button_selector', None)
        kwargs['selector'] = self.login_button_selector
        self.login_button_action = ScraperAction(**kwargs)



    def login_if_necessary(self, **kwargs):
        """
        Determines if you are on the login page.
        If you are - login and return True
        Otherwise do nothing and return False
        :return: True if login was performed and False if not logged in or if not on the login page.
        """
        if not kwargs.get('driver',None) is None:
            self.driver = kwargs.get('driver',None)
        try:
            scraper_input = ScraperInputFields(driver=self.driver,input_values=self.selectors,retry_count=self.retry_count, web_driver=kwargs.get('web_driver', None))
            if self.is_on_login_page(**kwargs):
                input = scraper_input.input_all_values(driver=self.driver)
                if input:
                    action_performed = False
                    for i in range(self.retry_count):
                        if not action_performed:
                            action_performed = self.login_button_action.exec(**kwargs)

                    if action_performed:
                        return {
                            'command':  'LOGIN',
                            'message': 'Login succeeded.'
                        }
                    else:
                        return {
                            'command': 'LOGIN',
                            'message': 'Login failed.'
                        }
            else:
                return {
                    'command': 'LOGIN',
                    'message': 'Login not attempted.'
                }
        except:
            print('error logging in  see trace.')
            traceback.print_exc()
            return {
                'command' : 'LOGIN',
                'message' : 'Login failed'
            }

    def exec(self,**kwargs):
        """
        Overridden exec from ScraperComponent

        :param kwargs:
        :return:
        """
        login_vals = self.login_if_necessary(**kwargs)
        self.respond(login_vals)
        return login_vals

    def is_on_login_page(self,**kwargs):
        """
        Selectors to define whether we are on  the login page
        All selectors must be found
        :return: True if the driver is on  the login page
        """
        if not kwargs.get('driver',None) is None:
            self.driver =  kwargs.get('driver',None)
        for key  in self.login_page_selectors:
            try:
               c = self.driver.find_element_by_css_selector(key)
               if c is None:
                   return False
            except:
                return False        ##just not on login page -- no big deal
            return True

    class Factory:
        def create(self, **kwargs): return ScraperLogin(**kwargs)
