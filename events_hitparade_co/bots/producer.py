from events_hitparade_co.bots.bot import HitParadeBot
from events_hitparade_co.messaging.messaging import MessagingQueue
import gc
import json
import time
import traceback
import pprint as pp
import threading
from events_hitparade_co.threads.messaging import Messaging

class HitParadeProducerBot(HitParadeBot):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.scraper_ids_created, self.scrapers_created = self.start_scrapers()
        self.set_bot_data( **kwargs )
        self.cache_manager = kwargs.get('cache_manager', None)
        self.output_connector = self.cache_output_component_func(type_id=kwargs.get( 'output', 'HitParadeCachePublisherOuput' ), **kwargs)
        self.hit_parade_command_processor = self.cache_output_component_func(**self.get_command_to(**kwargs))
        self.sleep_time = 5
        self.bot_type = kwargs.get( 'bot.type', 'producer' )
        self.state_storage_store_prop(prop='start_url', val=kwargs.get('scraper_url', None))
        self.pp = pp.PrettyPrinter(indent=4)
        self.state_storage_store_prop(prop='producer_id', val=self.id)
        kwargs['web_driver'] = self.web_driver
        self.messaging_thread = Messaging(**kwargs)
        self.messaging_thread.setDaemon(True)
        self.messaging_thread.start()
        self.state_storage_store_prop(prop='current_state', val={
            'listen_for_urls' : self.listen_for_urls,
            'scraping_page'   : False,
            'current_url'     : None
        })
        print('*************************************  HitParadeProducerBot  *************************************')


    def reload_resources(self, id=-1, forced=False):
        """
        Determines whether or not the WebScraper has exceeded memory threshold.
        If it has, it restarts the current WebScraper and continues with what ever task it was doing.
        :param id: int id of the web scraper to restart if necessary.
        :return:
        """
        if self.is_over_threshold() or forced:
            print('memory portfolio too high....quitting... removing [%s]  ' % (str(id) ))
            self.reset_bot_resources(new_scraper=self.restart_web_scraper( start_url=self.state_storage_get_prop('start_url'), id=self.bot_data.get('web_driver', {}).id))
            return True
        return False

    def next_msg(self):
        command, obj = MessagingQueue.wait_for_msg(id=self.id, direction='in', caller='HitParadeProducerBot')
        obj['data'] = json.loads(obj['data'].decode('utf-8'))
        return {
            'command' : command,
            'message' : obj
        }

    def send_complete(self):
        MessagingQueue.send_msg(id=self.messaging_thread.id, direction='in', cmd='SEND', d={'message': 'COMPLETE'}, caller=str(self.id))

    def is_recurring(self):
        return True

    def run(self):
        """
        Thread run method.
        :return:
        """
        self.state_storage_store_prop(prop='run_count', val=2)
        self.state_storage_store_prop(prop='exception_count', val=0)
        self.state_storage_store_prop(prop='run_kwargs', val={'run_count': self.state_storage_get_prop('run_count')})
        self.state_storage_store_prop(prop='is_started', val=True)
        if not self.stopped():
            if self.is_recurring():
                self.state_storage_store_prop(prop='exception_count', val=0)
                while not self.stopped() and (not self.state_storage_get_prop('exit_on_exception') or (self.state_storage_get_prop('exit_on_exception') and self.state_storage_get_prop('exception_count')==0)):
                    self.state_storage_store_prop(prop='output_dict', val=dict())
                    try:
                        if self.state_storage_get_prop('current_state').get('listen_for_urls', False):
                            print('producer is listening for urls....')
                            if self.state_storage_get_prop('current_state').get('scraping_page', False):
                                print( '**********************************************Not reading messages - currently scraping %s *******************************************************' % str( self.state_storage_get_prop('current_state')['current_url']))
                            else:
                                print('not currently scraping any pages. Current url %s ' % str(self.state_storage_get_prop('current_state').get('current_url', None)))
                                print( '<<<<<<<<waiting for new message...%s >>>>>>>>' % self.id  )
                                self.state_storage_store_prop(prop='command_message', val=self.next_msg())
                                if not self.state_storage_get_prop('command_message').get('command', None) == 'SHUTDOWN':
                                    self.web_driver.create_driver()
                                    # self.web_driver.driver.maximize_window()
                                    self.web_driver.driver.implicitly_wait(self.timeout)
                                    self.state_storage_store_prop(prop='json_data', val=self.state_storage_get_prop('command_message').get('message', None)['data'] )
                                    self.state_storage_append_val(prop='message_list', val=self.state_storage_get_prop('json_data'))
                                    self.state_storage_store_prop(prop='start_url', val=self.state_storage_get_prop('json_data').get( 'url', None ).strip())
                                    self.state_storage_store_prop(prop='current_state', dict_sub='current_url', val= self.state_storage_get_prop('start_url'))
                                    self.state_storage_store_prop(prop='current_state', dict_sub='scraping_page', val=True)
                                    self.state_storage_store_prop(prop='data_selector', val=self.state_storage_get_prop('json_data').get( 'data_selector', None ))
                                    self.state_storage_store_prop(prop='publish_to', val=self.state_storage_get_prop('json_data').get( 'publish_to', None ))
                                    if  self.state_storage_get_prop('start_url'):
                                        print( 'fetching %s ' %  self.state_storage_get_prop('start_url') )
                                        self.state_storage_store_prop(prop='scraper_url', val=self.state_storage_get_prop('start_url'))
                                        self.state_storage_store_prop(prop='data_selectors', val=self.state_storage_get_prop('json_data').get('data_selector', {}).get('data_selectors', {}))
                                        self.state_storage_store_prop(prop='data_selectors',dict_sub='scraper_url', val=self.state_storage_get_prop('start_url'))
                                        self.state_storage_store_prop(prop='run_kwargs', val={ 'run_count': self.state_storage_get_prop('run_count'), 'scraper_url': self.state_storage_get_prop('start_url') , 'data_selectors': self.state_storage_get_prop('data_selectors') , 'bot_data' : self.bot_data})
                                        c, v = self.run_commands(**self.state_storage_get_prop('run_kwargs'))
                                        print('response_keys are....[%s]' % c)
                                        print(v.keys())
                                        self.state_storage_store_prop(prop='output_dict', dict_sub='publish_to', val=self.state_storage_get_prop('json_data').get('publish_event', None))
                                        self.state_storage_store_prop(prop='output_dict', dict_sub='recursive', val=self.state_storage_get_prop('json_data').get('recursive', False))
                                        v['scraper_url'] = self.state_storage_get_prop('start_url')
                                        v[self.state_storage_get_prop('json_data').get('id_property', 'id')] = MessagingQueue.unique_id(global_id=True, cache_manager=self.cache_manager)
                                        v['id_property'] = self.state_storage_get_prop('json_data').get('id_property', 'id')
                                        v['parent_id_property'] = self.state_storage_get_prop('json_data').get('parent_id_property', 'id')
                                        v['publish_to_event'] = self.state_storage_get_prop('json_data').get('publish_to_event', None)
                                        print('***************************************************************** output sent to %s for url %s ******************************************************************************' %(str(v['publish_to_event']), self.state_storage_get_prop('start_url')) )
                                        if v.get('current_url', None) and not v.get('scraper_url', None) == v.get('current_url', None):
                                            print('scraper_url and current_url mismatch [%s,%s]' % ( v.get('scraper_url', None) , v.get('current_url', None)))
                                            v['scraper_url'] = v['current_url']
                                        elif  v.get('current_url', None):
                                            print('urls are in synch at %s - %s' % (v.get('scraper_url', None), v.get('current_url', None)))
                                        if  v.get('current_url', None):
                                            print( '------------------------  publishing url on publish to to  [%s --> %s --> %s ] ------------------------' % (self.state_storage_get_prop('start_url'), self.state_storage_get_prop('output_dict')['publish_to'], self.state_storage_get_prop('output_dict').get('publish_to_event', None)))
                                            self.output_data(output=v, **self.state_storage_get_prop('output_dict'))
                                            self.state_storage_store_prop(prop='current_state', dict_sub='scraping_page', val=False)
                                            self.state_storage_store_prop(prop='start_url', val=None)
                                            self.state_storage_store_prop(prop='output_dict', val=dict())
                                            self.send_complete()
                                            print('<< [%s] releasing producer lock >> ' % (str(self.id)))
                                        else:
                                            print('current_url is none')
                                    self.web_driver.release_driver()
                                    self.state_storage_increment_val(prop='run_count', val=1)
                                    # del c
                                    # del v
                                    # gc.collect()
                                else:
                                    print('.')
                        else:
                            print( '**************************** ERROR PRODUCER BOT UNTESTED CODE *************************************')
                            if  self.state_storage_get_prop('start_url'): #self.start_url:
                                self.state_storage_store_prop(prop='scraper_url', val=self.state_storage_get_prop('start_url'))
                                self.state_storage_store_prop(prop='data_selectors', val=self.state_storage_get_prop('json_data').get('data_selector', {}).get('data_selectors', {}))
                                self.state_storage_store_prop(prop='data_selectors',dict_sub='scraper_url', val=self.state_storage_get_prop('start_url'))
                                c, v = self.run_commands(**self.state_storage_get_prop('run_kwargs'))
                                self.output_data(output=v)
                            self.state_storage_increment_val(prop='run_count', val=1)
                    except:
                        traceback.print_exc()
                        self.state_storage_increment_val(prop='exception_count', val=1)
                        print('<< [%s] releasing producer lock [EXCEPTION] >> ' % (str(self.id)))


    def run_recurring(self):
        return True
