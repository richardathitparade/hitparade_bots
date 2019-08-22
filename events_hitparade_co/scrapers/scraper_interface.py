from events_hitparade_co.messaging.messaging import MessagingQueue
from abc import abstractmethod
import threading
from threading import Thread
import traceback
from events_hitparade_co.registration.registration import RegisterLeafClasses
from selenium.webdriver.common.action_chains import ActionChains
class WebScraper(Thread):
    __metaclass__ = RegisterLeafClasses
    DEFAULT_WEB_SCRAPER_TIMEOUT = 5
    """
    Abstract class that defines a threaded WebScraper that may or may not run headless.
    Extend this class for each browser.
    """
    def __init__(self, **kwargs):
        """

        :param headless: True if you want your browser to run headless and false if you want to see the browser running
        :param start_url: str start url
        :param timeout: 5 if you want
        :param id: int id the web scraper should use.  If it is -1, generate a new id from MessagingQueue
        """
        # Initialize the thread
        Thread.__init__(self)
        self.__dict__ = dict(list(kwargs.items()) + list(self.__dict__.items()))
        for k in kwargs.keys():
            if kwargs.get(k, None) and isinstance(kwargs.get(k,None), str):
                print( ' %s ==> %s ' % (k, kwargs.get(k, None)) ) 
        self.cache_manager = kwargs.get('cache_manager' , None)
        self.create_driver()
        # self.driver.maximize_window()
        self.timeout = kwargs.get('timeout', None)
        self.driver.implicitly_wait(self.timeout)
        self.action = ActionChains(self.driver)
        if kwargs.get('id', -1) == -1:
            self.id = MessagingQueue.unique_id(global_id=True, cache_manager=self.cache_manager)
            print('unique id is now %s ' % str(self.id))
        else:
            self.id = kwargs.get('id', -1)
        if self.state_storage_get_prop('start_url'):
            self.set_headless()
        self.state_storage_store_prop( prop='start_url', val=kwargs.get('start_url', None) )
        self.default_parser = kwargs.get('default_parser', None)
        self._stop_event = threading.Event()
        self.scraping_lock = threading.Lock()
        self.get_external_ip_adresss = kwargs.get('get_external_ip_adresss', None)
        self.ip = kwargs.get('ip', None)
        self.get_state_static_prop = kwargs.get('get_state_static_prop', None)
        self.store_state_static_prop = kwargs.get('store_state_static_prop', None)
        self.driver.get('https://www.google.com')






    def get_id(self):
        """
        returns the id of the WebScraper
        :return: int id > 0
        """
        return self.id

    def stop(self):
        """
        Signals the WebScraper event to stop the WebScraper.
        :return:
        """
        self._stop_event.set()

    def stopped(self):
        """
        Returns True is the WebScraper is stopped
        Returns False if the WebScraper is not stopped.
        :return:
        """
        return self._stop_event.is_set()

    @abstractmethod
    def set_headless(self):
        """
        sets the scraper to be headless
        override for each browser
        """
        pass

    def quit_silently(self):
        try:
            self.driver.quit()
            return True
        except:
            traceback.print_exc()
            return False


    @abstractmethod
    def create_driver(self):
        """
        creates the appropriate web driver
        """
        pass


    def quit(self):
        """
        Called on a WebScraper in order to quit the WebsScraper and relinquish its resources.
        :return:
        """
        try:
            self.driver.quit()
            self.stop()
            return True
        except:
            traceback.print_exc()
            return False

    def respond(self, obj=None, command=None):
        """
        Sends a message to the main thread.
        :param obj: dict to send to the main thread
        :param command: str command being used.
        :return:
        """
        MessagingQueue.send_msg(id=self.get_id() , direction='out', cmd=command, d=obj, caller=str(self.get_id()))

    def change_url(self, new_url=None):
        if not new_url is None:
            self.state_storage_store_prop(prop='start_url', val=new_url.strip())
            self.driver.get(self.state_storage_get_prop('start_url') )

    def run(self):
        """
        Run the thread
        """
        try:
            if not self.state_storage_get_prop('start_url') is None:
                self.driver.get( self.state_storage_get_prop('start_url') )
        except:
            traceback.print_exc()
            print('error opening start url of %s ' % self.state_storage_get_prop('start_url') )
        ERROR_MESSAGE = False
        QUIT = False
        print('[%s] Scraper running...' % str(self.get_id()))
        scraper_component = None
        last_command = None
        id_value = self.get_id() if not self.get_id() is None else MessagingQueue.unique_id(global_id=True, cache_manager=self.cache_manager)
        while not ( ERROR_MESSAGE or QUIT ) and not self.stopped():
            print('[%s] Thread message loop ' % str(id_value))
            command, obj = MessagingQueue.wait_for_msg(id=self.id, direction='in', caller=str(id_value) )
            print('<<acquire producer scraper lock>>')
            self.scraping_lock.acquire()
            obj['driver'] = self.driver
            obj['get_state_static_prop'] = self.get_state_static_prop
            obj['store_state_static_prop'] = self.store_state_static_prop
            obj['id'] = self.id
            obj['web_driver'] = self
            type_id_value = obj.get('type_id', None)
            del obj['type_id']
            obj['cache_manager'] = self.cache_manager
            obj['ip'] = self.ip
            obj['get_external_ip_addresss'] = self.get_external_ip_adresss
            print('ip is %s ' % self.ip)
            obj['open_url']=False
            obj['default_parser'] = self.default_parser
            obj['nocommand'] = True
            for k in self.__dict__.keys():
                if 'storage_' in k:
                    obj[k] = self.__dict__[k]
            if scraper_component is None:
                scraper_component = self.cache_manager.cache_output_component_func(type_id=type_id_value, **obj)
            else:
                scraper_component.reset(**obj)

            print('[%s] :: command in %s with message %s '  %  (str(self.get_id()), command, str(obj)))
            if not self.stopped() or command == 'QUIT':
                print('[%s] Thread command either thread has been stopped or command is QUIT {%s} ' % ( str(self.get_id()), command ) )
                response_object = scraper_component.exec(**obj)
                if response_object is None:
                    print('response object is none.')
                    print(command)
                    print(obj)
                    self.respond( obj=obj, command=command )
                else:
                    self.respond( obj=response_object, command=command )
            else:
                print(  'no longer active...quitting...'  )
            print('<<release producer scraper lock>>')
            self.scraping_lock.release()
        if not  self.stopped():
            print('This Web Scraper %s is no longer active.  Shutting down. ' % self.id )

        q = self.quit()
        print('id[%s] quitting.....[%s]' % ( str(self.id), str(q)))
        MessagingQueue.quit(id=self.get_id(), caller=str(self.get_id()))
