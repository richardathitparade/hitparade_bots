from threading import Thread
import threading
from events_hitparade_co.bots.bot import HitParadeBot
from events_hitparade_co.messaging.messaging import MessagingQueue
import time
import traceback
class Messaging(HitParadeBot):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.message_lock = threading.Lock()
        self.subscribe_to_events( subscribable_events=[self.subscribable_event] )
        self.set_available()
        self.set_bot_data( **kwargs )


    def next_msg(self):
        message = self.event_subscriptions[self.subscribe_event]['subscriber'].next_message()
        return message



    def set_available(self):
            if isinstance(self.publish_event, str):
                self.cache_manager.publish_data( event=self.publish_event,
                                              recursive=False,
                                              output=str(self.id),
                                              pid=str(self.id),
                                              append_pid=False)
            elif isinstance(self.publish_event, dict):
                self.cache_manager.publish_data( event=self.publish_event.get('name', self.subscribable_event.get('event', None) ),
                                              recursive=self.publish_event.get('recursive', False),
                                              output=str(self.id),
                                              pid=str(self.id),
                                              append_pid=self.publish_event.get('append_pid', False))

            elif isinstance(self.publish_event, list):
                for list_event in self.publish_event:
                    if isinstance(list_event, dict):
                        self.cache_manager.publish_data(event=list_event.get('name', list_event.get('event', None)),
                                                        recursive=list_event.get('recursive', False),
                                                        output=str(self.id),
                                                        pid=list_event.get('pid', list_event.get('id', self.id)),
                                                        append_pid=list_event.get('append_pid', False))
                    elif isinstance(list_event, str):
                        self.cache_manager.publish_data(event=list_event,
                                                        recursive=False,
                                                        output=str(self.id),
                                                        pid=str(self.id),
                                                        append_pid=False)

    def run(self):
        exception_count = 0
        while not self.stopped() and (not self.state_storage_get_prop('exit_on_exception') or (self.state_storage_get_prop('exit_on_exception') and exception_count == 0)):
            #acquire lock
            print('<<messaging lock acquire [%s] producer [%s]>>' % (self.id, self.state_storage_get_prop('producer_id')))
            self.message_lock.acquire()
            #read message from cache
            message = self.next_msg()
            #check the message
            if message and not isinstance(message['data'], int) and not message['data'].decode('utf-8').isdigit():
                #send message to producer thread
                MessagingQueue.send_msg(id=self.state_storage_get_prop('producer_id'), direction='in', cmd='SEND', d=message,caller=str(self.id))
                #listen to completion message from thread
                command, obj = MessagingQueue.wait_for_msg(id=self.id, direction='in', caller='Messaging')
                #release lock
            print('<<messaging lock release [%s] producer [%s]>>' % (self.id,  self.state_storage_get_prop('producer_id')))
            self.message_lock.release()
            print('setting available from id %s for producer %s ' % (self.id,  self.state_storage_get_prop('producer_id')))
            self.set_available()
            #sleep
            time.sleep(self.sleep_time)