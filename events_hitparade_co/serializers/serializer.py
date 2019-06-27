from abc import abstractmethod
from events_hitparade_co.registration.registration import RegisterLeafClasses
class HitParadeSerializer:
    __metaclass__ = RegisterLeafClasses

    def __init__(self, **kwargs):
        self.__dict__ = dict(list(kwargs.items()) + list(self.__dict__.items()))

    @abstractmethod
    def store(self, **kwargs):
        pass