from abc import abstractmethod
from events_hitparade_co.registration.registration import RegisterLeafClasses
from events_hitparade_co.messaging.messaging import MessagingQueue
from datetime import timedelta, datetime
class UrlGenerator:
    __metaclass__ = RegisterLeafClasses

    def __init__(self, **kwargs):
        self.__dict__ = dict(list(kwargs.items()) + list(self.__dict__.items()))
        if self.data_selector_id:
            self.data_selector = self.state_storage_get_prop(self.data_selector_id)
        self.publisher_id = kwargs.get('publisher_id', -1)



    def generate_next_url(self, **kwargs):
        if self.state_storage_get_prop(prop='number_urls') <  self.state_storage_get_prop(prop='iterator_count'):
            return None, None, None, None, None, None, None
        else:
            publish_event = kwargs.get('publish_event' , self.publish_to_event )
            return_value = self.base_url
            cdi = self.state_storage_get_prop( prop='current_date_index' )
            cdi_date = datetime.strptime(cdi.split(' ')[0], self.url_pattern)
            if self.state_storage_get_prop( prop='iterator_count' ) > 0:
                return_value = self.base_url.format( new_date=cdi_date.strftime( self.date_pattern ) )
            update_cdi = cdi_date - timedelta( days=int(self.step_count) )
            self.state_storage_store_prop( prop='current_date_index', val=update_cdi )
            self.state_storage_increment_val( prop='iterator_count', val=self.step_count )
            id_property = self.id_property
            parent_id_property =  self.parent_id_property
            id_value = MessagingQueue.unique_id(global_id=True, cache_manager=self.cache_manager)
            parent_id_value =  None
            return return_value, self.data_selector, publish_event, id_property, parent_id_property, id_value, parent_id_value

    class Factory:
        """
        Factory class used by the UrlGenerator
        """
        def create(self, **kwargs): return UrlGenerator( **kwargs )