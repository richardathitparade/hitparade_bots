from events_hitparade_co.commands.command import WebScraperComponentCommand
class WebScraperComponentOpenCommand(WebScraperComponentCommand):
    """
    Class to open a url.  Used by ScraperComponent through a factory method.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute_web_command(self, **kwargs):
        if self.command == 'OPEN':
            return self.get_result_message(self.action.open())

    class Factory:
        def create(self, **kwargs): return WebScraperComponentOpenCommand(**kwargs)
