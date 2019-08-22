from events_hitparade_co.messaging.messaging import MessagingQueue
from events_hitparade_co.registration.registration import RegisterLeafClasses
from abc import abstractmethod
class HitParadeBotCommandProcessor:
    __metaclass__ = RegisterLeafClasses
    """
    Class deinfed to send commands to a WebScraper to perform an action or scrape and retrieve data.
    Default commands are defined within the class.
    """
    def __init__(self, **kwargs):
        """
        Constructor with kwargs as the data
        :param kwargs: Data Dictonary to pull data from.
        """
        self.bot_data = kwargs.get('bot_data', {})
        self.bot  = kwargs.get('bot', None)
        self.cache = kwargs.get('cache', None)
        self.command_processors = kwargs.get('command_processors', [])
        self.scraper = kwargs.get('scraper', None) if not kwargs.get('scraper', None) is None else self.bot.scrapers[0]
        self.caller = 'HitParadeBotCommandProcessor-Default::' if self.bot is None or self.bot.get_name() is None else self.bot.get_name()
        self.cache_manager = kwargs.get('cache_manager', self.bot_data.get('cache_manager', None))

    def run_command(self,id=-1, command=None, d=None, caller=None):
        """
        Sends a command to the proper MessagingQueue with the proper data object and command string.
        After a successful send if the object is not processing a QUIT command, the HitParadeBotCommandProcessor then listens to an incoming queue
        where the WebScraper sends a reply as to the success or failure of the command.

        The QUIT command is simply sent and all queues associated with the id are destroyed.
        :param id: int ID of the webscraper.
        :param command: str Command to perform.
        :param d: dict data dictionary of the command request
        :param caller: Thread calling this action usually  the main thread.  Used for logging purposes.
        :return:
        """
        MessagingQueue.send_msg(id=id, direction='in', cmd=command, d=d, caller=caller)
        command = command
        val = None
        if not command == 'QUIT':
            command, val = MessagingQueue.wait_for_msg(direction='out', id=id, caller=caller)
        print('command %s  ' % command)
        return command, val

    def login(self,id=-1, selectors=None, login_page_selectors=None, login_button_selector=None, caller=None, **kwargs):
        """
        Atomic Scraping Method
        Sends the login command to a WebScraper object.

        :param id: int id of the WebScraper to send to.
        :param selectors: list[str] list of selectors for username/password
        :param login_page_selectors: list[str] list of selectors to identify if you are on the login page.
        :param login_button_selector: str selector indicating the button to click to login.
        :param caller: str of the calling Thread - usually the Main thread.
        :return:  [2 values] command, value
        """
        message_dict = dict()
        message_dict['selectors'] = selectors
        message_dict['login_page_selectors'] = login_page_selectors
        message_dict['login_button_selector'] = login_button_selector
        message_dict['command'] = 'LOGIN'
        message_dict['type_id'] = 'ScraperLogin'
        message_dict['unique_id'] = MessagingQueue.unique_id(cache_manager=self.cache_manager)
        message_dict.update(kwargs)
        return self.run_command(id=id, command='LOGIN', d=message_dict, caller=caller)

    def open_url(self,id=-1, url=None, caller=None, **kwargs):
        message_dict = dict()
        message_dict['command'] = 'OPEN'
        message_dict['type_id'] = 'ScraperAction'
        message_dict['scraper_url'] = url
        message_dict['unique_id'] = MessagingQueue.unique_id(cache_manager=self.cache_manager)
        message_dict.update(kwargs)
        return self.run_command(id=id, command='OPEN', d=message_dict, caller=caller)


    def scrape_data(self,id=-1, scraping_props=None, caller=None, **kwargs):
        """
        Atomic Scraping Method
        Command that tells a WebScraper to Scrape Data from a URL.
        :param id: int id of the WebScraper to command.
        :param scraping_props: dict all properties to scrape.
        :param caller: str usually main thread
        :return:   [2 values] command, value
        """
        message_dict = dict()
        message_dict['scraper_logins'] = scraping_props.get( 'scraper_logins', [] )
        message_dict['data_selectors'] = scraping_props.get( 'data_selectors', None )
        message_dict['force_refresh'] = scraping_props.get( 'force_refresh', False )
        message_dict['web_driver'] = scraping_props.get( 'web_driver', None )
        message_dict['command'] = 'SCRAPE'
        message_dict['type_id'] = 'Scraper'
        message_dict['unique_id'] = MessagingQueue.unique_id(cache_manager=self.cache_manager)
        message_dict.update(kwargs)
        return self.run_command(id=id, command='SCRAPE', d=message_dict, caller=caller)

    def quit(self,id=-1, caller=None, **kwargs):
        """
        Atomic Scraping Method
        Command that tells the WebScraper to quit and shutdown.
        This command will force MessagingQueues associated with the id to be destroyed.

        :param id: int id of the WebScraper
        :param caller: str caller of the command usually Main Thread
        :return:   [2 values] command, value
        """
        message_dict = dict()
        message_dict['command'] = 'QUIT'
        message_dict['type_id'] = 'ScraperAction'
        message_dict['unique_id'] = MessagingQueue.unique_id(cache_manager=self.cache_manager)
        message_dict.update(kwargs)
        return self.run_command(id=id, command='QUIT', d=message_dict, caller=caller)

    @abstractmethod
    def run_cmd(self, **kwargs):
        """
        Runs any command sent to it.
        Command Processor command to run.
        May combine the default commands or create a new command in this method.
        This is the method that the HitParadeBot object calls.

        :param kwargs: dict of data to use if necessary.
        :return:   [2 values] command, value
        """
        pass
