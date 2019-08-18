from events_hitparade_co.bots.bot import HitParadeBot
import gc
import json
import time
from random import randrange
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
        self.get_state_static_prop = kwargs.get('get_state_static_prop', None) 
        kwargs['web_driver'] = self.web_driver
        self.messaging_thread = Messaging(**kwargs)
        self.messaging_thread.setDaemon(True)
        self.messaging_thread.start()
        self.sc = 0
        self.state_storage_store_prop(prop='current_state', val={
            'listen_for_urls' : self.listen_for_urls,
            'scraping_page'   : False,
            'current_url'     : None
        })
        self.ip = kwargs.get('ip', None)
        self.get_external_ip_addresss = kwargs.get('get_external_ip_addresss', None)
        print('PRODUCER IP ADDRESS is %s ' % self.ip)
        print('*************************************  HitParadeProducerBot  *************************************')

    def republish_format(self, json_data=None, republish_css=None, id_property='id', parent_id_property='parent_id', parent_id=None, parent_url=None):
        republish_css_dict = dict()
        republishers = republish_css.get('republishers', [])
        for republisher_id in republishers:
            self.republish( json_data=json_data, prop_id='buckets', republisher_css=republish_css.get(republisher_id, []), id_property=id_property, parent_id_property=parent_id_property, parent_id=parent_id, parent_url=parent_url )
        return self.__get_all_republish_urls(parent_id=parent_id)
             

    def republish(self,json_data=None, prop_id='general', republisher_css=None, id_property='id', parent_id_property='parent_id', parent_id=None, parent_url=None):
        if json_data is None:
            if republisher_css is None or len(republisher_css) == 0: 
                print('***** republisher_css is none and json_data is none')
            else:
                print('***** json_data is none and republisher_css is %s ' % str(republisher_css)) 
        elif json_data and republisher_css and len(republisher_css) > 0:
            css_listing = republisher_css[0]
            css_listing_value = republisher_css[0].get('css', None)
            css_listing_type = css_listing.get('type', None)
            if css_listing_value and css_listing_type:
                if css_listing_type == 'list':
                    if isinstance(json_data, list):
                        for el in json_data:
                            new_el = None
                            if el and isinstance(el, dict):
                                new_el = el.get( css_listing_value, None )
                            else:
                                new_el = el
                            if new_el and len(republisher_css)>1:
                                self.republish( json_data=new_el,  prop_id='buckets',republisher_css=republisher_css[1:], id_property=id_property, parent_id_property=parent_id_property, parent_id=parent_id, parent_url=parent_url )
                            else:
                                print('***** new_el is none or len republisher_css <= 1 keys[%s], %s' % ( str(  el.keys() ) , str( len( republisher_css) ) ) )
                    elif isinstance(json_data, dict):
                        json_data_value = json_data.get( css_listing_value, None )
                        if json_data_value and len(republisher_css)>1:
                            self.republish( json_data=json_data_value,  prop_id='buckets',republisher_css=republisher_css[1:], id_property=id_property, parent_id_property=parent_id_property, parent_id=parent_id, parent_url=parent_url )
                        else:
                            print('***** json_data_value is none or len republisher_css is <=1 keys[%s], %s ' % ( str(json_data.keys()), str( len(republisher_css) ) ) )
                    else:
                        print('***** else 1 %s '% type(json_data))
                        print('***** jsondata is %s ' % json_data)
                elif css_listing_type == 'object':
                    collection = self.state_storage_get_prop('data_selectors').get('collection', None)
                    database = self.state_storage_get_prop('data_selectors').get('database', None)
                    reformatter = self.state_storage_get_prop('data_selectors').get('reformatter', None)
                    database_serializer = self.state_storage_get_prop('data_selectors').get('database_serializer', None)
                    dict_value = {
                        'parent_id_property': parent_id, 
                        'data_selector_id' : css_listing.get('data_selector_id', None),
                        'model_event_id': css_listing.get('model_event_id', None),
                        'id_property': id_property,
                        'parent_url': parent_url,
                        'parent_id_property': parent_id_property,
                        'parent_id' : parent_id,
                        'collection' : collection,
                        'database' : database,
                        'reformatter' : reformatter,
                        'database_serializer' : database_serializer
                    }
                    if isinstance( json_data, list ):
                        for el in json_data:
                            if el and isinstance(el, dict):
                                el_value =  el.get(css_listing_value, None)
                            else:
                                el_value = el 
                            if el_value:
                                dict_value['id'] = self.unique_id( global_id=True, cache_manager=self.cache_manager )
                                dict_value['url'] = css_listing.get('base_url', None) + el.get( css_listing_value, None )
                                dict_value['database_serializer']   = self.__dict__[dict_value['data_selector_id']].get('database_serializer', None)
                                dict_value['reformatter']           = self.__dict__[dict_value['data_selector_id']].get('reformatter', None)
                                dict_value['collection']            = self.__dict__[dict_value['data_selector_id']].get('collection', None)
                                dict_value['database']              = self.__dict__[dict_value['data_selector_id']].get('database', None)
                                dict_value['add_range'] = 1
                                self.add_statics_listitem(prop_id=prop_id, val=dict_value, separate=True)
                            else:
                                print('***** el_value is none not adding')
                                print('***** el keys are  %s' % str(el.keys()))
                    elif isinstance( json_data, dict ):
                        d_value = json_data.get(css_listing_value, None)
                        if d_value:
                            dict_value['id'] = self.unique_id( global_id=True, cache_manager=self.cache_manager )
                            dict_value['url'] = css_listing['base_url'] + dict_value
                            dict_value['database_serializer']   = self.__dict__[dict_value['data_selector_id']].get('database_serializer', None)
                            dict_value['reformatter']           = self.__dict__[dict_value['data_selector_id']].get('reformatter', None)
                            dict_value['collection']            = self.__dict__[dict_value['data_selector_id']].get('collection', None)
                            dict_value['database']              = self.__dict__[dict_value['data_selector_id']].get('database', None)
                            dict_value['add_range'] = 2
                            self.add_statics_listitem(prop_id=prop_id, val=dict_value, separate=True)
                        else:
                            print('***** not adding dict_value it is none and d_value keys are %s'% str(json_data.keys()))
                    elif isinstance( json_data, str ):
                        print('***** ------------------- str  ------------------ ' % str(json_data) )
                    else: 
                        print('***** else 2 %s '% type(json_data))
                        print('***** jsondata is %s ' % json_data)

            
                
        
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
        command, obj = self.wait_for_msg(id=self.id, direction='in', caller='HitParadeProducerBot')
        obj['data'] = json.loads(obj['data'].decode('utf-8'))
        return {
            'command' : command,
            'message' : obj
        }

    def send_complete(self):
        self.send_msg(id=self.messaging_thread.id, direction='in', cmd='SEND', d={'message': 'COMPLETE'}, caller=str(self.id))

    def is_recurring(self):
        return True



    def get_runkwargs(self, **kwargs):
        print( 'runkwargs get url_value is %s' % kwargs.get('url_value', None) )
        return  {
                          'run_count': self.state_storage_get_prop('run_count'),
                          'scraper_url': kwargs.get('url_value', None) ,
                          'data_selectors': self.state_storage_get_prop('data_selectors') ,
                          'bot_data' : self.bot_data,
                          'ip': self.ip,
                          'get_external_ip_addresss': self.get_external_ip_addresss
                      }

    def get_urlz(self, json_data=None):
        url_value = json_data.get( 'url', None ).strip()
        print('get_urlz get url_value is %s ' % url_value)
        return url_value, self.get_state_static_prop(prop=url_value , default_value=None, dict_sub=None), self.get_state_static_prop(prop=url_value + '.status' , default_value=None, dict_sub=None)


    def store_state_vals(self, message_list=None, url_value=None, scraping=True, data_selector=None, publish_to=None):
        self.state_storage_append_val(prop='message_list', val=message_list)
        self.state_storage_store_prop(prop='current_state', dict_sub='current_url', val=url_value)
        self.state_storage_store_prop(prop='current_state', dict_sub='scraping_page', val=scraping)
        self.state_storage_store_prop(prop='data_selector', val=data_selector)
        self.state_storage_store_prop(prop='publish_to', val=publish_to)
        if not '?' in url_value and not 'hp_ts=' in url_value:
            url_value_timestamp = url_value + '?hp_ts={timestamp}'.format( timestamp = str(time.time() * randrange(1000000) ).split('.')[0])
            print('timestamp added - url is now url_value_ts %s ' % url_value_timestamp )
            self.state_storage_store_prop(prop='start_url', val=url_value_timestamp) 
        else:
            print('url already has timestamp of %s ' % url_value)
            self.state_storage_store_prop(prop='start_url', val=url_value) 


    def poststate(self, **kwargs):
        v = kwargs.get('scraped_result', None)
        if not v.get('current_url', None) is None and ('http://' in  v.get('current_url', None) or 'https://' in v.get('current_url', None)):
            json_data = kwargs.get('json_data', None)
            self.state_storage_store_prop(prop='output_dict', dict_sub='publish_to', val=json_data.get('publish_event', None))
            self.state_storage_store_prop(prop='output_dict', dict_sub='recursive', val=json_data.get('recursive', False))
            v[json_data.get('id_property', 'id')] = self.unique_id(global_id=True, cache_manager=self.cache_manager)
            v['id_property'] = json_data.get('id_property', 'id')
            v['parent_id_property'] = json_data.get('parent_id_property', 'id')
            v[ v['id_property'] ] = json_data.get( v['id_property'], None )
            v[ v['parent_id_property'] ] = json_data.get( v['parent_id_property'], None )
            v['publish_to_event'] = json_data.get('publish_to_event', None)
            self.store_state_static_prop(prop=str(v[ v['id_property'] ]) +'.hash', val=v['hash'] , dict_sub=None)
            self.store_state_static_prop(prop=str(v[ v['id_property'] ]) +'.url', val=v['current_url'] , dict_sub=None)
            self.store_state_static_prop(prop=str(v[ v['id_property'] ]) +'.ip', val=str(self.ip) , dict_sub=None)
            self.store_state_static_prop(prop=v['hash'] +'.id', val=str(v[ v['id_property'] ]) , dict_sub=None)
            self.store_state_static_prop(prop=v['current_url'] +'.id', val=str(v[ v['id_property'] ]) , dict_sub=None)
            self.store_state_static_prop(prop=str(self.ip) +'.id', val=str(v[ v['id_property'] ]) , dict_sub=None)


    def __get_all_republish_urls(self, parent_id=None):
        return [obj for obj in self.get_statics_listitems() if obj.get('parent_id', None) == parent_id]

    def __run_scr(self, url_value=None, **run_kwargs):
        c, v = self.run_commands(**run_kwargs)
        print('url value for scrape is %s ' % url_value )
        url_verified = self.get_state_static_prop(prop=v['hash'] , default_value=None, dict_sub=None)
        print('********** url_verified is %s , %s ********** **********' % ( type(url_verified), str(url_verified) ) )
        counter = 0
        while isinstance( url_verified, str) and not url_value in url_verified or isinstance( url_verified, list) and not url_value in url_verified:
            c, v = self.run_commands(**run_kwargs)
            url_verified = self.get_state_static_prop(prop=v['hash']+'.fullvalue' , default_value=None, dict_sub=None)
            print('********** ********** url_verified is %s , %s ********** **********  ' % ( type(url_verified), str(url_verified) ) )
            url_verified_wurl = self.get_state_static_prop(prop=v['hash']+'.wurl' , default_value=None, dict_sub=None)
            print('********** ********** url_verified is now %s ********** ********** ' % str(url_verified))
            print('********** ********** url_verified wurl is now %s ********** ********** ' % url_verified_wurl)
            print('********** ********** url_value is now %s ********** ********** ' % url_value)
            if url_verified is None:
                url_verified = self.get_state_static_prop(prop=v['hash']  , default_value=None, dict_sub=None)
        return c, v, url_verified


    def synch_results(self, scraped_result=None):
        if scraped_result.get('current_url', None) and not scraped_result.get('scraper_url', None) == scraped_result.get('current_url', None):
            print('scraper_url and current_url mismatch [%s,%s]' % ( scraped_result.get('scraper_url', None) , scraped_result.get('current_url', None)))
            scraped_result['scraper_url'] = scraped_result['current_url']
        elif  scraped_result.get('current_url', None):
            print('urls are in synch at %s - %s' % (scraped_result.get('scraper_url', None), scraped_result.get('current_url', None)))

    def republish_if_necessary(self, scraped_result=None, json_data=None):
        if  scraped_result.get('current_url', None):
            if json_data.get( 'data_selector', None ) and json_data.get( 'data_selector', None ).get('data_selectors', None) and json_data.get( 'data_selector', None ).get('data_selectors', None).get('republisher_css', None):
                republish_urls = self.republish_format(json_data=scraped_result, parent_url=scraped_result['current_url'] , parent_id=json_data.get( scraped_result['id_property'], None ), republish_css=json_data.get( 'data_selector', None ).get('data_selectors', None).get('republisher_css', None))
                scraped_result['republish_urls'] = republish_urls
                self.output_data(output=scraped_result, **self.state_storage_get_prop('output_dict'))
            elif json_data.get( 'data_selector', None ) and json_data.get( 'data_selector', None ).get('data_selectors', None) and json_data.get( 'data_selector', None ).get('data_selectors', None).get('republisher_css', None) is None:
                self.output_data(output=scraped_result, **self.state_storage_get_prop('output_dict'))

    def reset_resources(self, scrape_command=None, scraped_result=None, run_kwargs=None, url_value=None, url_verified=None, url_verified_status=None, json_data=None):
        self.state_storage_store_prop(prop='current_state', dict_sub='scraping_page', val=False)
        self.state_storage_store_prop(prop='start_url', val=None)
        self.state_storage_store_prop(prop='output_dict', val=dict())
        self.send_complete()
        self.state_storage_increment_val(prop='run_count', val=1)
        self.sc = 0
        self.__del_rscs(scrape_command=scrape_command, scraped_result=scraped_result, run_kwargs=run_kwargs, url_value=url_value, url_verified=url_verified, url_verified_status=url_verified_status, json_data=json_data)
        print('<< [%s] releasing producer lock >> ' % (str(self.id)))

    def __del_rscs(self, scrape_command=None, scraped_result=None, run_kwargs=None, url_value=None, url_verified=None, url_verified_status=None, json_data=None):
        del scrape_command
        del scraped_result
        del run_kwargs
        del url_value
        del url_verified
        del url_verified_status
        del json_data
        gc.collect()
 

    def __sleep(self):
        print( 'sleeping %s seconds' % str(self.sleep_time) )    
        time.sleep( self.sleep_time )
        self.sc += self.sleep_time

    def __thread_setup(self):
        self.state_storage_store_prop(prop='run_count', val=2)
        self.state_storage_store_prop(prop='exception_count', val=0)
        self.state_storage_store_prop(prop='run_kwargs', val={'run_count': self.state_storage_get_prop('run_count')})
        self.state_storage_store_prop(prop='is_started', val=True)

    def run(self):
        """
        Thread run method.
        :return:
        """
        self.__thread_setup()
        if not self.stopped():
            if self.is_recurring():
                self.state_storage_store_prop(prop='exception_count', val=0)
                while not self.stopped() and (not self.state_storage_get_prop('exit_on_exception') or (self.state_storage_get_prop('exit_on_exception') and self.state_storage_get_prop('exception_count')==0)):
                    self.state_storage_store_prop(prop='output_dict', val=dict())
                    try:
                        if self.state_storage_get_prop('current_state').get('listen_for_urls', False):
                            print('producer is listening for urls....')
                            if not self.state_storage_get_prop('current_state').get('scraping_page', False):
                                self.state_storage_store_prop(prop='current_state', dict_sub='listen_for_urls', val=True)
                                print('not currently scraping any pages. Current url %s ' % str(self.state_storage_get_prop('current_state').get('current_url', None)))
                                print( '<<<<<<<<waiting for new message...%s >>>>>>>>' % self.id  )
                                self.state_storage_store_prop(prop='command_message', val=self.next_msg())
                                if not self.state_storage_get_prop('command_message').get('command', None) == 'SHUTDOWN':
                                    self.state_storage_store_prop(prop='current_state', dict_sub='scraping_page', val=True)
                                    json_data_value = self.state_storage_get_prop('command_message').get('message', None)['data']
                                    url_value, url_verified, url_verified_status = self.get_urlz(json_data=json_data_value)
                                    self.store_state_vals(message_list=json_data_value , url_value=url_value, scraping=True, data_selector=json_data_value.get( 'data_selector', None ), publish_to=json_data_value.get( 'publish_to', None ))
                                    run_kwargs = self.get_runkwargs(url_value=url_value)
                                    c = v = None
                                    if  url_value and url_verified_status is None and url_verified is None:
                                        self.state_storage_store_prop( prop='scraper_url', val=url_value )
                                        self.state_storage_store_prop(prop='data_selectors', val=json_data_value.get('data_selector', {}).get('data_selectors', {}))
                                        self.state_storage_store_prop(prop='data_selectors',dict_sub='scraper_url', val=url_value)
                                        c,v,url_verified = self.__run_scr(url_value=url_value, **run_kwargs)
                                        self.poststate(scraped_result=v, json_data=json_data_value)
                                        self.synch_results(scraped_result=v)
                                        self.republish_if_necessary(scraped_result=v, json_data=json_data_value)
                                        self.reset_resources(scrape_command=c, scraped_result=v, run_kwargs=run_kwargs, url_value=url_value, url_verified=url_verified, url_verified_status=url_verified_status, json_data=json_data_value)
                                        print('<< [%s] releasing producer lock >> ' % (str(self.id)))
                            print('sleep....%s' % str(self.sleep_time)) 
                            self.__sleep()
                            if self.sc > 25:
                                self.reset_resources()
                    except:
                        traceback.print_exc()
                        self.state_storage_increment_val(prop='exception_count', val=1)
                        print('<< [%s] releasing producer lock [EXCEPTION] >> ' % (str(self.id)))

    def run_recurring(self):
        return True
