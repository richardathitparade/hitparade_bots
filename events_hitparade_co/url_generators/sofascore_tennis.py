from events_hitparade_co.messaging.messaging import MessagingQueue
from events_hitparade_co.url_generators.generator import UrlGenerator
from datetime import timedelta
class SofaScoreMainUrlGenerator(UrlGenerator):

    def __init__(self, **kwargs):
        super().__init__( **kwargs )

    def generate_next_url(self, **kwargs):
        if self.state_storage_get_prop(prop='number_urls') <  self.state_storage_get_prop(prop='iterator_count'):
            return None, None, None, None, None, None, None
        else:
            publish_event = kwargs.get('publish_event' , self.publish_to_event )
            return_value = self.base_url
            if self.state_storage_get_prop( prop='iterator_count' ) > 0:
                return_value = self.base_url.format( new_date=self.state_storage_get_prop(prop='current_date_index').strftime(self.date_pattern) )
            update_cdi = self.state_storage_get_prop( prop='current_date_index' ) - timedelta( days=self.step_count )
            self.state_storage_store_prop( prop='current_date_index', val=update_cdi )
            self.state_storage_increment_val( prop='iterator_count', val=self.step_count )
            id_property = self.id_property
            parent_id_property =  self.parent_id_property
            id_value = MessagingQueue.unique_id(global_id=True, cache_manager=self.cache_manager)
            parent_id_value =  None
            return return_value, self.data_selector, publish_event, id_property, parent_id_property, id_value, parent_id_value

    class Factory:
        """
        Factory class used by the SofaScoreMainUrlGenerator
        """
        def create(self, **kwargs): return SofaScoreMainUrlGenerator( **kwargs )
