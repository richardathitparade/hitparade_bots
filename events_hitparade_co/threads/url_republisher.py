from events_hitparade_co.messaging.messaging import MessagingQueue
from threading import Thread
import time
import traceback
class HitParadeUrlRepublisher(Thread):

    def __init__(self, **kwargs):
        Thread.__init__(self)
        update_props = dict(list(self.__dict__.items()) + list(kwargs.items()))
        self.__dict__ = dict(list(update_props.items()) + list(self.__dict__.items()))
        self.id = MessagingQueue.unique_id(global_id=True, cache_manager=self.cache_manager)
        self.event_subscriptions = dict()
        self.subscribable_event  = kwargs.get( 'subscribable_event',  None )
        if isinstance(self.subscribable_event, list):
            self.subscribe_event = self.subscribable_event
        else:
            self.subscribe_event = self.subscribable_event if isinstance(self.subscribable_event, str) else self.subscribable_event.get('event', self.subscribable_event.get('name', None))
            if isinstance(self.subscribe_event, list):
                self.subscribe_event = self.subscribe_event[0]
        republisher_type_id = list(filter(lambda u: u.get('url_event_id', None) == self.url_event_id,self.url_republisher_data ))[0].get('type_id', None)
        self.url_republisher_dict = list(filter(lambda u: u.get('url_event_id', None) == self.url_event_id,self.url_republisher_data ))[0]
        del self.url_republisher_dict['type_id']
        self.url_republisher = self.cache_manager.cache_output_component_func(type_id=republisher_type_id, **self.url_republisher_dict)
        self.url_republisher_dict['type_id'] = republisher_type_id
        r, rc = self.cache_manager.subscribe_to(event=self.subscribe_event, recursive=False, pid=self.id, append_pid=False)
        if isinstance(self.subscribe_event, str):
            self.event_subscriptions[self.subscribe_event] = self.event_subscriptions.get(self.subscribe_event, {})
            self.event_subscriptions[self.subscribe_event]['subscriber'] = rc if r else None
            self.event_subscriptions[self.subscribe_event]['result'] = r if r else False
        elif isinstance(self.subscribe_event, list):
            for se in self.subscribe_event:
                self.event_subscriptions[se] = self.event_subscriptions.get(se, {})
                self.event_subscriptions[se]['subscriber'] = rc if r else None
                self.event_subscriptions[se]['result'] = r if r else False
        print('*************************************  HitParadeUrlRepublisher  *************************************')

    def _listen_run_sleep_loop(self, subscribe_event=None):
        message = self.event_subscriptions[subscribe_event]['subscriber'].next_message()
        if not message is None and not isinstance(message.get('data', None), int):
            data_dict = dict()
            data_dict['data'] = message.get('data', None)
            # todo: add serializer
            match_urls_object, data_selector, publish_event = self.url_republisher.republish(**data_dict)
            if message.get('data', None) and match_urls_object and match_urls_object.get('match_urls', []) and len( match_urls_object.get('match_urls', []) ) > 0:
                parent_id_value = None
                id_value = None
                if match_urls_object.get( 'parent_id_property', None ):
                    parent_id_value = match_urls_object.get(match_urls_object.get('parent_id_property', None), None)
                if match_urls_object.get('id_property', None):
                    id_value = match_urls_object.get(match_urls_object.get('id_property', None), None)
                obj_dict = dict()
                obj_dict['id_property'] = match_urls_object.get('id_property', None)
                obj_dict['parent_id_property'] = match_urls_object.get('parent_id_property', None)
                if match_urls_object.get('id_property', None):
                    obj_dict[match_urls_object.get('parent_id_property', None)] = id_value
                obj_dict['urls'] = match_urls_object.get('match_urls', [])
                obj_dict['data_selector'] = data_selector.get('data_selectors', None)
                obj_dict['publish_event'] = publish_event
                MessagingQueue.send_msg(id=self.publisher_id, direction='in', cmd='SEND', d=obj_dict,
                                        caller=str(self.id))
                print('urls sent %s ' % (str(match_urls_object.get('match_urls', []))))
        time.sleep(self.sleep_time)

    def run(self):
        while True:
            try:
                if isinstance(self.subscribe_event, str):
                    self._listen_run_sleep_loop(subscribe_event=self.subscribe_event)
                elif isinstance(self.subscribe_event, list):
                    for se in self.subscribe_event:
                        self._listen_run_sleep_loop(subscribe_event=se)
                    # message = self.event_subscriptions[self.subscribe_event]['subscriber'].next_message()
                    # if not message is None and not isinstance(message.get('data', None), int):
                    #     data_dict = dict()
                    #     data_dict['data'] = message.get( 'data', None )
                    #     #todo: add serializer
                    #     match_urls_object, data_selector, publish_event  = self.url_republisher.republish( **data_dict )
                    #     if message.get('data', None) and match_urls_object and match_urls_object.get('match_urls', []) and len(match_urls_object.get('match_urls', [])) > 0:
                    #         parent_id_value = None
                    #         id_value = None
                    #         if  match_urls_object.get('parent_id_property', None):
                    #             parent_id_value = match_urls_object.get( match_urls_object.get('parent_id_property', None),  None )
                    #         if match_urls_object.get('id_property', None):
                    #             id_value =  match_urls_object.get( match_urls_object.get('id_property', None) , None )
                    #         obj_dict = dict()
                    #         obj_dict['id_property'] = match_urls_object.get('id_property', None)
                    #         obj_dict['parent_id_property'] = match_urls_object.get('parent_id_property', None)
                    #         if match_urls_object.get('id_property', None):
                    #             obj_dict[ match_urls_object.get('id_property', None) ] = id_value
                    #         if match_urls_object.get('parent_id_property', None):
                    #             obj_dict[ match_urls_object.get('parent_id_property', None) ] = parent_id_value
                    #         obj_dict['urls'] = match_urls_object.get( 'match_urls', [] )
                    #         obj_dict['data_selector'] = data_selector.get('data_selectors', None)
                    #         obj_dict['publish_event'] = publish_event
                    #         MessagingQueue.send_msg(  id=self.publisher_id, direction='in', cmd='SEND', d=obj_dict , caller=str(self.id)  )
                    #         print('urls sent %s ' % (str( match_urls_object.get('match_urls', []) )))
                    #     time.sleep(self.sleep_time)
            except:
                traceback.print_exc()
                pass
