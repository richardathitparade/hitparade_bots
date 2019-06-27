from events_hitparade_co.components.component import ScraperComponent
import traceback
class ScraperInputFields(ScraperComponent):
    """
    Class that inputs fields into the forms on a page.
    Override this if you need to perform custom inputs.
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
        super().__init__(**kwargs)
        self.input_values = kwargs.get('input_values',None)

    def input_all_values(self, driver=None):
        """
        Input the values n the forms.
        For weird fields, override this function with a custom ScraperInputField
        :return True if all the fields were input False if one was not
        """
        if not driver is None:
            self.driver = driver
        for key,value in self.input_values.items():
            try:
                self.driver.find_element_by_css_selector(key).send_keys(value)
            except:
                traceback.print_exc()
                print('exception setting %s to %s ' % (key,value))
                return False
        return True

    def exec(self,**kwargs):
        """
        Overridden exec from ScraperComponent

        :param kwargs:
        :return:
        """
        input_vals = self.input_all_values(**kwargs)
        self.respond(input_vals)
        return input_vals

    class Factory:
        def create(self, **kwargs): return ScraperInputFields(**kwargs)
