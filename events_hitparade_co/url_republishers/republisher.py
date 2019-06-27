from abc import abstractmethod
import json
import traceback
from events_hitparade_co.registration.registration import RegisterLeafClasses
class UrlRepublisher:
    __metaclass__ = RegisterLeafClasses

    def __init__(self, **kwargs):
        update_props = dict(list(self.__dict__.items()) + list(kwargs.items()))
        self.__dict__ = dict(list(update_props.items()) + list(self.__dict__.items()))
        if self.data_selector_id:
            self.data_selector = self.state_storage_get_prop(self.data_selector_id)

    def load_data_from_string(self, property='data', **kwargs):
            try:
                return json.loads(kwargs.get(property, None))
            except:
                traceback.print_exec()
                return None

    @abstractmethod
    def republish(self, **kwargs):
        pass
