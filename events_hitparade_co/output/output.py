from events_hitparade_co.registration.registration import RegisterLeafClasses
from abc import abstractmethod
class HitParadeOutput:
    __metaclass__ = RegisterLeafClasses
    """
    Class responsible for taking data and transmitting it to the proper medium be it a file, systemio, database or cache.
    Extend this class to add another data connector.

    A HitParadeBot object calls the output method of a HitParadeOutput object as an abstraction layer.
    """
    TYPES = ['print','cache','db']
    DEFAULT_TYPE='print'
    def __init__(self, **kwargs):
        __metaclass__ = RegisterLeafClasses
        self.type=kwargs.get('type', HitParadeOutput.DEFAULT_TYPE)


    @abstractmethod
    def output(self,output=None, **kwargs):
        """
        Outputs the data passed in
        :param output: Data passed in
        :return: nothing
        """
        pass
