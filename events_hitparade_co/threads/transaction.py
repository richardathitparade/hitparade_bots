from events_hitparade_co.messaging.messaging import MessagingQueue
from events_hitparade_co.threads.connectors.transaction_manager import HitParadeTransactionManager
from threading import Thread
import time

class HitParadeTransactionThread(Thread):

    def __init__(self, **kwargs):
        Thread.__init__(self)
        self.__dict__ = dict(list(kwargs.items()) + list(self.__dict__.items()))
        self.__dict__[self.id_property] = MessagingQueue.unique_id(global_id=True, cache_manager=self.cache_manager)
        self.get_state_static_prop = kwargs.get('get_state_static_prop', None)
        self.store_state_static_prop = kwargs.get('store_state_static_prop', None)
        self.cache_output_component_func = kwargs.get('cache_output_component_func', None)
        self.transaction_processor = HitParadeTransactionManager(**kwargs)
        self.sleep_time = 30                                                                                        #autoset to 30 seconds
        print('*********************************  HitParadeTransactionThread  *********************************')



    def run(self):
        while True:
            print('*********************************  HitParadeTransactionThread  - transaction - ********************************* ')
            self.transaction_processor.load_transactions( )
            time.sleep(self.sleep_time)
