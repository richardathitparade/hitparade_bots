from events_hitparade_co.messaging.messaging import MessagingQueue
import json
from threading import Thread
import time
import traceback


class HitParadeConsumerThread(Thread):

    def __init__(self, **kwargs):
        Thread.__init__(self)
        self.__dict__ = dict(list(kwargs.items()) + list(self.__dict__.items()))
        self.__dict__[self.id_property] = MessagingQueue.unique_id(global_id=True, cache_manager=self.cache_manager)
        self.event_subscriptions = dict()
        self.subscribe_to_events()
        print('*********************************  HitParadeConsumerThread  *********************************')

    def subscribe_to_events(self):
        store_from = self.events.get(self.model_event_id, None).get('store_from')
        if isinstance(store_from, str):
            self.event_subscriptions = self.subscribe_to_event(event=store_from, recursive=False, pid=self.__dict__[self.id_property], append_pid=False)
        elif isinstance(store_from, list):
            for publish_event in store_from:
                if isinstance(publish_event, str):
                    self.event_subscriptions = self.subscribe_to_event(event=publish_event, recursive=False, pid=self.__dict__[self.id_property], append_pid=False)
                elif isinstance(publish_event, dict):
                    self.event_subscriptions = self.subscribe_to_event(event=publish_event.get('name', publish_event.get('event', None)), recursive=False,  pid=self.__dict__[self.id_property], append_pid=False)

    def load_data_from_string(self, property='data', **kwargs):
            try:
                return json.loads(kwargs.get(property, None))
            except:
                traceback.print_exec()
                return None

    def subsribe_to_one_event( self, event=None, recursive=False, pid=None, append_pid=False ):
        try:
            print('------------------ subscribing to cache %s ------------------ ' % (event))
            return self.cache_manager.subscribe_to( event=event, recursive=recursive, pid=pid, append_pid=append_pid )
        except:
            print('************subsribe_to_one_event***************')
            traceback.print_exc()
            return False, None

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
            if isinstance(event, str):
                event_subscription_results[event] = dict()
                event_subscription_results[event]['result'] = True if result else False
                event_subscription_results[event]['subscriber'] = result_subscriber if result else None
            elif isinstance(event, list):
                for e in event:
                    if isinstance(e, str):
                        event_subscription_results[e] = dict()
                        event_subscription_results[e]['result'] = True if result else False
                        event_subscription_results[e]['subscriber'] = result_subscriber if result else None
                    elif isinstance(e, dict):
                        e_name = e.get('name', e.get('event', None))
                        event_subscription_results[e_name] = dict()
                        event_subscription_results[e_name]['result'] = True if result else False
                        event_subscription_results[e_name]['subscriber'] = result_subscriber if result else None
        return event_subscription_results

    def run(self):
        while True:
            subscribe_to_events = self.events.get(self.model_event_id, None).get('store_from')
            if isinstance(subscribe_to_events, str):
                message = self.event_subscriptions[subscribe_to_events]['subscriber'].next_message()
                if message and message.get('data', None) and not isinstance(message['data'], int):
                    message_dict = dict()
                    message_dict['data'] = self.load_data_from_string(**message)
                    consumer_args = self.__dict__
                    if not message_dict['data'].get('scraper_url', None) is None:
                        message_dict['filename'] = subscribe_to_events + '.' + message_dict['data'].get('scraper_url', None).split('/')[-1] + '.' + message_dict['data'].get(message_dict['data'].get('id_property', None), None) + '.json'
                        self.cache_output_component_func.create(**consumer_args).store(**message_dict)
                else:
                    print('hitparadeconsumer - noop for event %s' % subscribe_to_events)
            elif isinstance(subscribe_to_events, list):
                for evt in subscribe_to_events:
                    message = self.event_subscriptions[evt]['subscriber'].next_message()
                    if message and message.get('data', None) and not isinstance(message['data'], int):
                        message_dict = dict()
                        message_dict['data'] = self.load_data_from_string(**message)
                        consumer_args = self.__dict__
                        if not message_dict['data'].get('scraper_url', None) is None:
                            message_dict['filename'] = evt + '.' +  message_dict['data'].get('scraper_url', None).split('/')[-1] + '.' +  message_dict['data'].get( message_dict['data'].get('id_property', None), None) + '.json'
                            self.cache_output_component_func.create(**consumer_args).store(**message_dict)  #process_model(**message_dict)
                    else:
                        print('hitparadeconsumer - noop for event %s' % evt)
            time.sleep(self.sleep_time)
