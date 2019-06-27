from abc import abstractmethod
from events_hitparade_co.registration.registration import RegisterLeafClasses
class WebScraperComponentCommand:
    __metaclass__ = RegisterLeafClasses
    """
    Base class of WebScraperComponentCommand
    Must impliment execute_web_method
    Used by ScraperComponent
    """

    def __init__(self, **kwargs):
        self.command = kwargs.get('command', None)
        self.action = kwargs.get('action', None)

    def get_result_message(self, success):
        if success:
            return {
                'success': success,
                'command': self.command,
                'message': 'Successfully ' + self.command + '.'
            }
        else:
            return {
                'success': success,
                'command': self.command,
                'message': 'Failed to ' + self.command + '.'
            }

    @abstractmethod
    def execute_web_command(self, **kwargs):
        pass
