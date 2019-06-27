from events_hitparade_co.components.component import ScraperComponent
class ScraperAction(ScraperComponent):
    """
    ScraperAction is what a bot uses to click buttons, enter inputs and call other ScraperComponents.
    It can also scroll or use javascript.
    Class that will open the url if necessary.
    Find the selected selector.
    And action it in the form of a button.
    Custom Actions should extend this class.
    """
    def __init__(self,**kwargs):
        """
        Constructor for ScraperAction
        :param driver:  WebScraper driver
        :param web_driver: WebScraper
        :param scraper_url: str url
        :param selector: str selector
        :param command: str command being run.
        :param retry_count: int number of times to retry an action.
        """
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
                **    selector: str selector to use inside this ScraperAction
        """
        super().__init__(**kwargs)
        self.selector = kwargs.get('selector', None)
        self.wait_for = kwargs.get('wait_for', False)
        self.time_delay = kwargs.get('time_delay', 3)
        self.use_once = kwargs.get('use_once', False)
        self.use_until_failure = kwargs.get('use_until_failure', False)
        self.finished = False

    def exec(self,**kwargs):
        """
        Overridden exec from ScraperComponent

        :param kwargs:
        :return:
        """
        return self.perform_action(**kwargs)


    def perform_action(self, **kwargs):
        """
        Performs action sepecified.

        :return: True if performed and False if not successfully performed
        """
        if not kwargs.get('driver', None) is None:
            self.driver =  kwargs.get('driver', None)
        if self.command is None:
            self.command = 'SCRAPE'
        command_dict = {
            'action' : self,
            'command' : self.command
        }
        return self.cache_output_component_func(**command_dict).execute_web_command(**command_dict)


    class Factory:
        def create(self, **kwargs): return ScraperAction(**kwargs)
