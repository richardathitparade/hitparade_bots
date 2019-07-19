from events_hitparade_co.messaging.messaging import MessagingQueue
from threading import Thread
import time
import traceback
class UrlPublisher(Thread):
    DEFAULT_PUBLISHER_SUFFIX = '.processors'

    def __init__(self, **kwargs):
        Thread.__init__( self )
        self.__dict__ = dict(list(kwargs.items()) + list(self.__dict__.items()))
        self.event_subscriptions = dict()
        self.id = kwargs.get('publisher_id', None) if not  kwargs.get('publisher_id', None)  is None else MessagingQueue.unique_id(global_id=True)
        self.publisher_id = self.id
        if not self.subscribable_event is None:
            self.subscribe_event = self.subscribable_event if isinstance(self.subscribable_event, str) else self.subscribable_event.get('event', self.subscribable_event.get('name', None))
        else:
            self.subscribe_event = None
        self.subscribe_event = self.subscribe_event + self.url_publisher_suffix if self.url_publisher_suffix is not None and not self.url_publisher_suffix in self.subscribe_event else  self.subscribe_event
        r, rc = self.cache_manager.subscribe_to(  event=self.subscribe_event, recursive=False, pid=self.id, append_pid=False )
        self.event_subscriptions[self.subscribe_event] = dict()
        self.event_subscriptions[self.subscribe_event]['subscriber'] = rc if r else None
        self.event_subscriptions[self.subscribe_event]['result'] = r if r else False
        print('url generators are %s ' % str( self.url_generators ) )
        self.url_generator_data = list( filter( lambda f: f.get('url_event_id', None)==self.url_event_id, self.url_generators ) )[0]
        self.url_generator_data['publish_event'] = self.publish_event
        self.url_generator_data['publish_to_event'] = kwargs.get( 'publish_to_event', None )
        self.url_generator_data.update(self.__dict__)
        self.get_state_static_prop = kwargs.get('get_state_static_prop', None)
        generator_type_id = self.url_generator_data['type_id']
        del self.url_generator_data['type_id']
        self.url_generator = self.cache_manager.cache_output_component_func(type_id=generator_type_id, **self.url_generator_data)
        self.url_generator_data['type_id'] = generator_type_id
        self.state_storage_store_prop(prop='offset_days', val=self.offset_days)
        self.state_storage_store_prop(prop='days_back', val=self.days_back)
        print('*************************************  UrlPublisher  *************************************')


    def generate_next_url(self, **kwargs):
        return self.url_generator.generate_next_url( **kwargs )

    def is_republished(self, url=None):
        return url in self.state_storage_get_prop(prop='received_urls').keys()

    def get_next_key(self):
        url_val = None
        for url in self.state_storage_get_prop(prop='received_urls').keys():
            url_val = url
            received_urls = self.state_storage_get_prop(prop='received_urls')
            url_status = received_urls[url_val]
            if url_status.get('status', None) is None:
                url_status['status'] = 'republishing'
                self.state_storage_store_prop(prop='received_urls',val=received_urls)
                received_urls[url_val] = url_status
                break
        return url_val,url_status

    def __get_next_url(self, process_id=None):
        url_value = None
        data_selector = None
        publish_to_event = None
        id_property = None
        parent_id_property = None
        id_value = None
        parent_id_value = None
        generate_url_args = dict()
        item_value = self.get_next_statics_listitem()
        if item_value:
            id_property = item_value['id_property']
            parent_id_property = item_value['parent_id_property']
            generate_url_args['publish_event'] = self.publish_to_event
            generate_url_args['publish_to_event'] = self.publish_to_event
            generate_url_args[id_property] = item_value[id_property] if item_value[id_property] else MessagingQueue.unique_id(global_id=True, cache_manager=self.cache_manager)
            url_value =  item_value['url']
            data_selector = self.__dict__[ item_value['data_selector_id'] ]
            id_value = generate_url_args[id_property]
            parent_id_value = item_value[parent_id_property]
            publish_to_event = self.events.get(item_value['model_event_id'], None).get('publish_to', None)
            url_check = url_value.split('?')[0] if '?' in url_value else url_value
            hash_value = self.get_state_static_prop(prop=url_check, default_value=None, dict_sub=None)
        else:
            generate_url_args['publish_event'] = self.publish_to_event
            generate_url_args['publish_to_event'] = self.publish_to_event
            generate_url_args['id'] = process_id if process_id else MessagingQueue.unique_id(global_id=True, cache_manager=self.cache_manager)
            url_value, data_selector, publish_to_event, id_property, parent_id_property, id_value, parent_id_value = self.generate_next_url( **generate_url_args )
            url_check = url_value.split('?')[0] if '?' in url_value else url_value
            hash_value = self.get_state_static_prop(prop=url_check, default_value=None, dict_sub=None)
        #if the url has already been scraped we go to the next.
        while not hash_value is None:
            print('url [%s] is already scraped '%url_value)
            url_value, data_selector, publish_to_event, id_property, parent_id_property, id_value, parent_id_value = self.generate_next_url( **generate_url_args )
            url_check = url_value.split('?')[0] if '?' in url_value else url_value
            hash_value = self.get_state_static_prop(prop=url_check, default_value=None, dict_sub=None)
        return url_value, data_selector, publish_to_event, id_property, parent_id_property, id_value, parent_id_value

    def run(self):
         while  self.state_storage_get_prop(prop='iterator_count') < (self.offset_days + self.days_back):
            try:
                message =  self.event_subscriptions[self.subscribe_event]['subscriber'].next_message()
                if message and not isinstance(message['data'], int) and message['data'].decode('utf-8').isdigit():
                    process_id = message['data'].decode('utf-8')
                    push_event = self.publish_event
                    ##change
                    url_value, data_selector, publish_to_event, id_property, parent_id_property, id_value, parent_id_value = self.__get_next_url()
                    if not url_value is None and not data_selector is None:
                        print(  'Sending %s scrape to process %s ' % (url_value, str(process_id))  )
                        publish_dict = dict()
                        publish_dict['url'] = url_value
                        publish_dict['id_property'] = id_property
                        publish_dict['parent_id_property'] = parent_id_property
                        publish_dict['scraper_process_id'] = process_id
                        publish_dict[ id_property ] =  id_value
                        publish_dict[ parent_id_property ] = parent_id_value
                        publish_dict['publish_to_event'] = publish_to_event
                        publish_dict['publish_event'] = self.events.get( self.events.get(self.model_event_id,None).get('url_id', None), None ).get('publish_to', None)
                        publish_dict['data_selector'] = data_selector
                        publish_dict['output_type'] = 'cache'
                        if isinstance(push_event, list):
                            for pe in push_event:
                                self.cache_manager.pub( event=pe, output=publish_dict, recursive=False, pid=process_id, append_pid=True)
                        else:
                            self.cache_manager.pub(event=publish_dict['publish_event'], output=publish_dict, recursive=False, pid=process_id, append_pid=True)
                else:
                    print('url-publisher listener no-op listening to %s ' % self.subscribe_event)
                print('*****            republisher values          *****')
                time.sleep(self.sleep_time)
            except:
                traceback.print_exc()
