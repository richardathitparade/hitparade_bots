from events_hitparade_co.messaging.messaging import MessagingQueue
from events_hitparade_co.registration.registration import RegisterLeafClasses
from abc import abstractmethod
import os
import psutil
import time
import threading
from threading import Thread
import traceback
from datetime import datetime
class HitParadeBot(Thread):
    __metaclass__ = RegisterLeafClasses
    """
    The HitParadeBot combines one or many custom commands to perform an action on the web and return a result.
    This will start with opening a URL.
    And combinations of:
    login
    scraping data
    clicking buttons and inputting data
    at the end of the operations, data and a command will be returned.

    QUIT command will be issued.

    A bot can be a one time bot that scrapes a page and ends for instance.
    A bot can be a recurring bot that runs every 40 seconds and scrapes the same page.

    As data is retrieved, data is sent to a HitParadeOutput object.
    If you wish to change where data is sent, please create a new HitParadeOutput
    object or use one of the default objects provided.
    """
    #Default number of retries for a web scraper
    DEFAULT_RETRY = 3
    #Deafult timeout for a web scraper
    DEFAULT_TIMEOUT = 5
    #Default type of web scraper
    DEFAULT_SCRAPER_TYPE = 'FirefoxWebScraper'
    #Default maximum memory threshold for this program before the web scraper is destroyed and restarted for memory efficiency.
    DEFAULT_MEMORY_THRESHOLD = 90000000
    #default scraper count.
    DEFAULT_SCRAPER_COUNT = 1
    #default sleep time for when a bot is recurring, how many seconds should the bot sleep between operations
    DEFAULT_SLEEP_TIME = 10
    #default exit_on_exception
    DEFAULT_EXIT_ON_EXCEPTION = False
    #default exit_on_exception
    DEFAULT_LISTEN_FOR_URLS = True
    #default exit_on_exception
    DEFAULT_RECURSIVE_SUBSCRIBE = False
    #default exit_on_exception
    DEFAULT_ADD_PROCESS_ID = False
    def __init__(self, **kwargs):
        """
        Constructor HitParadeBot which is a Thread
        IS-A Thread
        :param kwargs: The following properties are pulled from kwargs
        scrapers - [] list of active scrapers  default is []
        bot_data - dict of data for the bot to use. default is {}
        scraper_type - FirefoxWebScraper or ChromeWebScraper are the options.  default FireFoxWebScraper
        start_url - url to start with. Default is None
        memory_threshold - memory threshold to which the web scraper will be rebooted if it takes up too much memory.
        recurring - bool True if the Bot will be running in a loop for a certain amount of time, false otherwise.
        headless - bool True if the WebScraper should be headless and False if the WebScraper can be show.  Default is False.
        timeout - int timeout in seconds of a url.  Default  HitParadeBot.DEFAULT_TIMEOUT
        retry - int number of time s to retry a url or task.  Default  HitParadeBot.DEFAULT_RETRY
        output_connector - HitParadeOUtput object to output retrieved or scraped data from the bot. Default HitParadeDefaultOutput which pretty prints to the system/io
        number_scrapers - int number of WebScrapers to launch. Default HitParadeBot.DEFAULT_SCRAPER_COUNT
        exit_on_exception - bool True if you want the bot to exit on the first exception and False if you want the bot to continue despite the exception. Default is  HitParadeBot.DEFAULT_EXIT_ON_EXCEPTION
        sleep_time - int number of seconds to sleep on a recurring bot.  default is HitParadeBot.DEFAULT_SLEEP_TIME
        hit_parade_command_processor - HitParadeCommandProcessor to run.  Default is None
        """
        Thread.__init__(self)
        self.__dict__ = dict(list(kwargs.items()) + list(self.__dict__.items()))
        self.scrapers = kwargs.get('scrapers', [])
        self.cache_manager = kwargs.get('cache_manager', None)
        self.scraper_type = kwargs.get('scraper_type', HitParadeBot.DEFAULT_SCRAPER_TYPE)
        self.cache_output_component_func = kwargs.get('cache_output_component_func', None)
        self.event_subscriptions = dict()
        self.bot_data = kwargs.get('bot_data', None) if not kwargs.get('bot_data', None) is None else self.bot_data if not self.bot_data is None else dict()
        self.state_storage_store_prop(prop='start_url', val= kwargs.get('start_url', None) )
        self.process = psutil.Process(os.getpid())
        self.memory_threshold = kwargs.get('memory_threshold', HitParadeBot.DEFAULT_MEMORY_THRESHOLD)
        self.recurring = kwargs.get('recurring', True)
        self.is_init = True
        self.is_started = False
        self.timeout = kwargs.get('timeout', HitParadeBot.DEFAULT_TIMEOUT)
        self.retry = kwargs.get('retry', HitParadeBot.DEFAULT_RETRY)
        self.add_process_id = kwargs.get( 'add_process_id', HitParadeBot.DEFAULT_ADD_PROCESS_ID )
        self.id = MessagingQueue.unique_id(global_id=True, cache_manager=self.cache_manager)
        self.output_connector = self.cache_output_component_func(type_id=kwargs.get('output', 'HitParadeDefaultOuput' ), **kwargs) #HitParadeFactories.create(type_id=kwargs.get('output', 'HitParadeDefaultOuput' ), **kwargs)
        self.number_scrapers = kwargs.get('number_scrapers', HitParadeBot.DEFAULT_SCRAPER_COUNT)
        self._stop_event = threading.Event()
        self.exit_on_exception = bool(kwargs.get('exit_on_exception', HitParadeBot.DEFAULT_EXIT_ON_EXCEPTION)) if isinstance( kwargs.get('exit_on_exception', HitParadeBot.DEFAULT_EXIT_ON_EXCEPTION), str) else kwargs.get('exit_on_exception', HitParadeBot.DEFAULT_EXIT_ON_EXCEPTION)
        self.listen_for_urls = bool( kwargs.get('listen_for_urls', HitParadeBot.DEFAULT_LISTEN_FOR_URLS) ) if isinstance( kwargs.get('listen_for_urls', HitParadeBot.DEFAULT_LISTEN_FOR_URLS), str ) else kwargs.get( 'listen_for_urls', HitParadeBot.DEFAULT_LISTEN_FOR_URLS )
        self.sleep_time = kwargs.get('sleep_time', HitParadeBot.DEFAULT_SLEEP_TIME)
        self.recursive_subscribe = kwargs.get('recursive_subscribe', HitParadeBot.DEFAULT_RECURSIVE_SUBSCRIBE)
        self.hit_parade_command_processor = kwargs.get( 'command_processor', None )
        self.subscribable_event  = kwargs.get( 'subscribable_event',  None )
        self.subscribe_event = self.subscribable_event if isinstance(self.subscribable_event, str) else self.subscribable_event.get('event', self.subscribable_event.get('name', None))
        self.publish_event = kwargs.get('publish_event' , None )
        self.publish_to_event = kwargs.get('publish_to_event', None)
        self.bot_name = 'HitParadeBot[' + str(self.id) + ']'
        self.listen_for_urls =  kwargs.get( 'listen_for_urls', False )


    GLOBAL_SCRAPERS = []

    @staticmethod
    def get_scrapers():
        return HitParadeBot.GLOBAL_SCRAPERS

    def subscribe_to_events( self, subscribable_events=[] ):
        for subscribable_event in subscribable_events:
            subscribable_event_string = subscribable_event.get( 'event',  None )
            recursive_sub = subscribable_event.get( 'recursive', False )
            append_pid = subscribable_event.get( 'append_pid', self.add_process_id )
            print('------------------ subscribing to %s ------------------ ' % subscribable_event_string)
            # def subscribe_to( event=None, recursive=False, pid=None, append_pid=False ):
            event_config_results = self.subscribe_to_event( event=subscribable_event_string, recursive=recursive_sub, pid=self.id, append_pid=append_pid  )
            if event_config_results:
                self.event_subscriptions.update(event_config_results)
            else:
                print('Warning: Event in config file is NONE. please look into it.')

    def subscribe_to_event(self, event=None, recursive=False, pid=None, append_pid=False):
        event_subscription_results = dict()
        if recursive:
            event_s = ''
            for seg in event.split('.'):
                event_s += seg + '.'
                # def subscribe_to( event=None, recursive=False, pid=None, append_pid=False ):
                event_subscription_results[event_s[0:-1]] = {
                    'recursive': True,
                    'pid' : pid,
                    'append_pid' : append_pid,
                    'event' : event_s[0:-1]
                }
                result, result_subscriber = self.subsribe_to_one_event( event=event_s[0:-1], recursive=recursive, pid=pid, append_pid=append_pid )
                event_subscription_results[event_s[0:-1]] = dict()
                event_subscription_results[event_s[0:-1]]['result'] = True if result else False
                event_subscription_results[event_s[0:-1]]['subscriber'] = result_subscriber if result else None
        else:
            result, result_subscriber = self.subsribe_to_one_event( event=event, recursive=recursive, pid=pid, append_pid=append_pid )
            event_subscription_results[event] = dict()
            event_subscription_results[event]['result'] = True if result else False
            event_subscription_results[event]['subscriber'] = result_subscriber if result else None
        return event_subscription_results

    def subsribe_to_one_event( self, event=None, recursive=False, pid=None, append_pid=False ):
        try:
            print('------------------ subscribing to cache %s ------------------ ' % (event))
            return self.cache_manager.subscribe_to( event=event, recursive=recursive, pid=pid, append_pid=append_pid )
        except:
            print('************subsribe_to_one_event***************')
            traceback.print_exc()
            return False, None

    def set_bot_data(self, **kwargs):
        self.bot_data['cache_manager'] = self.cache_manager
        self.bot_data['web_driver'] = self.web_driver
        if not self.bot_data is None:
            self.bot_name = 'HitParadeBot[' + str(self.id) + ']' if self.bot_data.get('bot_name',  None) is None else self.bot_data.get( 'bot_name', None )

    def get_command_to(self, **kwargs):
        """
        Creates a dict as a transfer object to pass to the command processor.
        :param kwargs: dict in the event you wish to override this method.
        :return: dict as a transfer object to be used when creating a HitParadeCommandProcessor
        """
        command_processor_data = dict()
        command_processor_data['bot_data'] = self.bot_data
        command_processor_data['bot'] = self
        command_processor_data['scraper'] = kwargs.get('scraper', None) if not kwargs.get('scraper', None) is None else self.scrapers[0]
        command_processor_data['command_processors'] = None
        command_processor_data['cache_manager'] = self.cache_manager
        command_processor_data['type_id'] = kwargs.get('command_processor', 'HitParadeBotScrapeProcessor')
        return command_processor_data

    def start_scrapers(self):
        """
        Start the WebScrapers if necessary.
        :return:
        """
        scrapers_created = 0
        scraper_ids_created = []
        if len(self.scrapers) < self.number_scrapers:
            number_of_scrapers_to_create = self.number_scrapers - len(self.scrapers)
            for i in range(number_of_scrapers_to_create):
                new_scraper = self.start_web_scraper( start_url=None  )
                if not new_scraper is None:
                    scrapers_created += 1
                    scraper_ids_created.append(new_scraper.get_id())
        return scraper_ids_created, scrapers_created

    def stop(self):
        """
        Sends a stop signal to the HitParadeBot
        :return:
        """
        self._stop_event.set()

    def stopped(self):
        """
        Tells the uer if the HitParadeBot is stopped.
        :return:  True if stopped and False if not Stopped.
        """
        return self._stop_event.is_set()

    def get_id(self):
        """
        gets the id of the HitParadeBot
        :return:  int id
        """
        return self.id

    def output_data(self, output=None, event=None, **kwargs ):
        """
        Sends the output to the hitparade_command_processor
        :param output: dict or string to output
        :return:
        """
        print(output)
        output_type = kwargs.get('output_type', 'cache')
        if output_type:
            if output_type=='cache':
                evts = event if event else kwargs.get('event', kwargs.get('publish_to', None))
                if isinstance(evts, str):
                    print('(3) publishing to cache %s , output %s ' % (evts, str(output)))
                    self.cache_manager.publish_data( event=evts,
                                               recursive=kwargs.get('recursive', False),
                                               output=output,
                                               pid=self.id,
                                               append_pid=kwargs.get('append_pid', False))
                elif isinstance(evts, list):
                    for e in evts:
                        if isinstance(e, str):
                            print('(1) publishing to cache %s , output %s ' % (e, str(output)))
                            self.cache_manager.publish_data(event=e,
                                                      recursive=kwargs.get('recursive', False),
                                                      output=output,
                                                      pid=self.id,
                                                      append_pid=kwargs.get('append_pid', False))
                        elif isinstance(e, dict):
                            print('(2) publishing to cache %s , output %s ' % (e.get('name', e.get('event', None)), str(output)))
                            self.cache_manager.publish_data(event=e.get('name', e.get('event', None)),
                                                      recursive=e.get('recursive', False),
                                                      output=output,
                                                      pid=e.get('pid', e.get('id', self.id ) ),
                                                      append_pid=e.get('append_pid', False))
            else:
                print('not output to cache - using default outputter')
                self.output_connector.output( output )
        else:
            print('warning - no output type specified using default ouput connector.')
            self.output_connector.output( output )

    @abstractmethod
    def run_recurring(self):
        pass

    def run_commands(self, **kwargs):
        """
        Runs the hit_parade_command_processor command
        :param kwargs:
        :return:
        """
        if self.state_storage_get_prop(prop='start_url') is None:
            self.state_storage_store_prop(prop='start_url', val=kwargs.get('scraper_url', None).strip())
        return self.hit_parade_command_processor.run_cmd( **kwargs )

    def run(self):
        """
        Thread run method.
        :return:
        """
        self.is_started = True
        if not self.stopped():
            if self.is_recurring():
                exception_count = 0
                while not self.stopped() and self.run_recurring() and (not self.state_storage_get_prop('exit_on_exception') or (self.state_storage_get_prop('exit_on_exception') and exception_count==0)):
                    try:
                        self.reload_resources()
                        c, v = self.run_commands()
                        self.output_data(output=v)
                        time.sleep(self.sleep_time)
                    except:
                        traceback.print_exc()
                        exception_count += 1
            else:
                try:
                    self.reload_resources()
                    c, v = self.run_commands()
                    self.output_data(output=v)
                except:
                    traceback.print_exc()

    def reset_bot_resources(self, new_scraper=None):
        """
        Private helper method when restarting a WebScraper
        :param new_scraper:
        :return:
        """
        # if not new_scraper is None and not self.bot_data is None:
        #     self.bot_data['web_driver'] = new_scraper
        #     self.bot_data['driver'] = new_scraper.driver
        if not new_scraper is None:
            self.web_driver = new_scraper
            self.driver = new_scraper.driver
            return True
        return False

    def reload_resources(self, id=-1):
        """
        Determines whether or not the WebScraper has exceeded memory threshold.
        If it has, it restarts the current WebScraper and continues with what ever task it was doing.
        :param id: int id of the web scraper to restart if necessary.
        :return:
        """
        if self.is_over_threshold():
            print('memory portfolio too high....quitting... removing [%s]  ' % (str(id) ))
            self.reset_bot_resources(new_scraper=self.restart_web_scraper( start_url=self.state_storage_get_prop('start_url'), id=id))

    def get_memory_used(self):
        """
        Returns size of memory profile of this program
        :return: int size of the memory profile.
        """
        return int(self.process.memory_info().rss)

    def is_over_threshold(self):
        """
        Determines whether the current memory profile is larger than the memory threshold of the program.
        :return: True if memory used is > memory_threshold and False otherwise.
        """
        return self.get_memory_used() > self.memory_threshold

    def is_started(self):
        """
        Returns true if the bot is initialized and the webscrapers have been started.
        :return: True if bot is initialzed and web scrapers have been started.
        """
        return self.is_started

    def is_initialized(self):
        """

        :return:
        """
        return self.is_init

    def is_recurring(self):
        """
        identifies if this bot is a recurring bot or a one time bot.
        :return: True if is recurring and False if it is a one time bot.
        """
        return self.recurring

    def get_name(self):
        """
        Returns the name of the bot for logging.
        :return: str name of the bot.  Could be string version of the id or something a little more elaborate.
        """
        return self.bot_name if self.bot_name else None

    def start_web_scraper( self, start_url=None ):
        """
        Starts web scraper
        Started as daemon thread.
        added to the active scrapers.
        :param start_url: url to open
        :return: WebScraper that was started
        """
        unique_id = MessagingQueue.unique_id(global_id=True, cache_manager=self.cache_manager)
        print('thread id is %s ' % unique_id)
        kwargs_v = dict()
        kwargs_v['headless'] =  self.state_storage_get_prop('start_url')
        kwargs_v['scraper_type'] = self.scraper_type
        kwargs_v['timeout'] = self.timeout
        kwargs_v['start_url'] =  self.state_storage_get_prop('start_url')
        kwargs_v['chrome_binary'] =  self.state_storage_get_prop('chrome_binary')
        kwargs_v['google_chrome_binary'] =  self.state_storage_get_prop('google_chrome_binary')
        kwargs_v['cache_input_file'] =  self.state_storage_get_prop('cache_input_file')
        kwargs_v['cache_manager'] = self.cache_manager
        kwargs_v['cache_output_component_func'] = self.cache_output_component_func
        kwargs_v['id'] = unique_id
        kwargs_v['default_parser'] = self.default_parser
        for k in self.__dict__.keys():
            if 'state_' in k:
                kwargs_v[k] = self.__dict__[k]
        s = self.cache_output_component_func(type_id=self.scraper_type, **kwargs_v)
        self.default_parser.driver = s.driver
        s.setDaemon(True)
        self.scrapers.append(s)
        HitParadeBot.GLOBAL_SCRAPERS.append(s)
        self.driver = s.driver
        self.web_driver = s
        s.start()
        return s

    @staticmethod
    def find_scrap(id=-1):
        """
                Returns the scraper being searched for based upon the id of the scraper.
                :param id: int id of the scraper to search for.
                :return: WebScraper that is being searched for or None if the id is not found.
                """
        if id == -1 and len(HitParadeBot.GLOBAL_SCRAPERS) == 1:
            return HitParadeBot.GLOBAL_SCRAPERS[0]
        else:
            return list(filter(lambda s: s.get_id() == id,HitParadeBot.GLOBAL_SCRAPERS))

    def find_scraper(self,id=-1):
        return HitParadeBot.find_scrap(id=id)
            # if len(thread_to_find) == 1:
            #     return thread_to_find[0]
            # else:
            #     return None

    def restart_web_scraper( self, id=-1, start_url=None ):
        """
        restarts the web scraper based upon the id.
        sets the start_url to open upon the opening the new web scraper.
        :param id: int id of web scraper to remove.  if it is < 0, then we assume the scrapers
        :param start_url: str url to open
        :return: webscraper that has been newly created.
        """
        scraper_to_stop = self.find_scraper(id=id)
        if not scraper_to_stop is None:

            for s in scraper_to_stop:
                try:
                    print('Stopping thread %s ' % str(s.get_id()))
                    s.stop()
                    s.quit()
                except:
                    traceback.print_exc()

        else:
            print('Not Stopping thread %s.  Not found in list' % str(id))
        return self.start_web_scraper( start_url=start_url  )
