from events_hitparade_co.bots.bot import HitParadeBot
from datetime import datetime
from events_hitparade_co.command_processor.command_processor import HitParadeBotCommandProcessor
import gc
import json
import time
import traceback
class HitParadeConsumerBot(HitParadeBot):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.output_connector = self.cache_manager.cache_output_component_func(type_id='HitParadeCachePublisherOuputFeeder', **kwargs)
        kwargs['bot'] = self
        self.bot_type = kwargs.get( 'bot.type', 'consumer' )
        self.hit_parade_command_processor = kwargs.get('command_processor', None) if not kwargs.get('command_processor', None)  is None  and isinstance(kwargs.get('command_processor', None), HitParadeBotCommandProcessor ) else eval(kwargs.get('command_processor', None)+'.Factory()').create(**kwargs)
        self.sleep_time = 5
        if not self.hit_parade_command_processor is None:
            self.hit_parade_command_processor.bot = self
            self.hit_parade_command_processor.scraper = kwargs.get('scraper', None) if not kwargs.get('scraper', None) is None else self.scrapers[0]
        self.subscribe_to_events( subscribable_events=[self.subscribable_event] )
        # self.set_bot_data( **kwargs )
        print('*************************************  HitParadeConsumerBot  *************************************')


    def reload_resources(self, id=-1):
        """
        Determines whether or not the WebScraper has exceeded memory threshold.
        If it has, it restarts the current WebScraper and continues with what ever task it was doing.
        :param id: int id of the web scraper to restart if necessary.
        :return:
        """
        if self.is_over_threshold():
            print('memory portfolio too high....quitting... removing [%s]  ' % (str(id) ))
            self.reset_bot_resources( new_scraper=self.restart_web_scraper( start_url=self.state_storage_get_prop('start_url'), id=self.bot_data.get('web_driver', {}).id) )
            return True
        return False

    def run(self):
        """
        Thread run method.
        :return:
        """
        self.state_storage_store_prop(prop='run_count', val=2)
        self.state_storage_store_prop(prop='run_kwargs', val={ 'run_count' : self.state_storage_get_prop('run_count') })
        self.is_started = True
        if not self.stopped():
            if self.is_recurring():
                self.state_storage_store_prop(prop='exception_count', val=0)
                while not self.stopped() and (not self.state_storage_get_prop('exit_on_exception') or (self.state_storage_get_prop('exit_on_exception') and self.state_storage_get_prop('exception_count')==0)):
                    try:
                        self.state_storage_store_prop(prop='message', val=self.event_subscriptions[self.subscribe_event]['subscriber'].next_message())
                        if self.state_storage_get_prop('message') and not isinstance(self.state_storage_get_prop('message')['data'], int) and not self.state_storage_get_prop('message')['data'].decode( 'utf-8' ).isdigit():
                            self.state_storage_store_prop(prop='json_value', val= json.loads( self.state_storage_get_prop('message')['data'].decode('utf-8')  ) )
                            self.state_storage_increment_val(prop='run_count', val=1)
                            #write out to db here!
                            self.output_connector.output(output=self.state_storage_get_prop('json_value'))
                        time.sleep(self.sleep_time)
                    except:
                        traceback.print_exc()
                        self.state_storage_increment_val(prop='exception_count', val=1)

    def run_recurring(self):
        return True
