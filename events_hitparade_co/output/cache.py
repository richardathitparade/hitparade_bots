from events_hitparade_co.registration.registration import RegisterLeafClasses
from events_hitparade_co.output.output import HitParadeOutput
import json
class HitParadeCachePublisherOuput(HitParadeOutput):
    """
    The default HitParadeOutput object.
    This pretty prints data to the commandline unless the data is none.  Then it just prints 'None'
    """
    def __init__(self,**kwargs):
        """
        Pull data for an output from kwargs
        :param kwargs: Data Dictionary passed to HitParadeOutput
        """
        __metaclass__ = RegisterLeafClasses
        super().__init__(**kwargs)
        self.publish_to = kwargs.get('publish_to', None)
        self.cache = kwargs.get('cache', None)
        self.recursive = kwargs.get('recursive', True)


    def __publish_cache(self, event=None, output=None, recursive=False):
        if self.cache:
            self.cache.publish_data(event_string=event, payload=output, recursive=recursive)
        else:
            self.cache_manager.publish_data(event_string=event, payload=output, recursive=recursive)

    def output(self,output=None, **kwargs):
        """
        overridden output command for the HitParadeOutput object.
        :param output: dict or str object to process.
        :return: none
        """
        event = kwargs.get('event', self.publish_to)
        recursive = kwargs.get('recursive', self.recursive)
        if not output is None:
            self.__output_data( output=output, event=event, recursive=recursive )
        else:
            print(  '  OUTPUT CONNECTOR WARNING  :: output is none - no action...  '  )

    def __output_data( self, output=None, event=None, recursive=False ):
        if isinstance(event, list):
            for e in event:
                self.__output_dict_str(  event=e, output=output, recursive=recursive  )
        elif isinstance(event, str):
            self.__output_dict_str(  event=event, output=output, recursive=recursive  )
        elif isinstance(event, dict):
            event_string = event.get('event', None)
            r = event.get('recursive', None) if not event.get('recursive', None) is None else recursive
            self.__output_dict_str(  event=event_string, output=output, recursive=r  )

    def __output_dict_str( self, output=None,event=None,recursive=False ):
        if recursive:
            evt_str = ''
            for evt in event.split('.'):
                evt_str += evt + '.'
                if isinstance(output, str):
                    self.__publish_cache  (event=evt_str[0:-1], output=output, recursive=recursive)#self.cache.publish_data(  event_string=evt_str[0:-1], payload=output, recursive=recursive  )
                else:
                    self.__publish_cache(event=evt_str[0:-1], output=json.dumps(output), recursive=recursive)#self.cache.publish_data(  event_string=evt_str[0:-1], payload=json.dumps(output), recursive=recursive  )
        else:
            if isinstance(output, str):
                self.__publish_cache(event=event, output=output, recursive=recursive)#self.cache.publish_data(  event_string=event, payload=output, recursive=recursive  )
            else:
                self.__publish_cache(event=event, output=json.dumps(output), recursive=recursive)#self.cache.publish_data(  event_string=event, payload=json.dumps(output), recursive=recursive  )

    class Factory:
        """
        Factory class used by the HitParadeOutputFactory
        """
        def create(self, **kwargs): return HitParadeCachePublisherOuput(**kwargs)
