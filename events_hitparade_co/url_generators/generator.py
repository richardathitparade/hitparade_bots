from abc import abstractmethod
from events_hitparade_co.registration.registration import RegisterLeafClasses
class UrlGenerator:
    __metaclass__ = RegisterLeafClasses

    def __init__(self, **kwargs):
        self.__dict__ = dict(list(kwargs.items()) + list(self.__dict__.items()))
        if self.data_selector_id:
            self.data_selector = self.state_storage_get_prop(self.data_selector_id)
        self.publisher_id = kwargs.get('publisher_id', -1)

    @abstractmethod
    def generate_next_url(self, **kwargs):
        pass