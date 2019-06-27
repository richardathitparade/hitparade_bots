from events_hitparade_co.commands.command import WebScraperComponentCommand
class WebScraperComponentQuitCommand(WebScraperComponentCommand):
    """
    Class to shutdown a WebScraper.  Used by ScraperComponent through a factory method.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute_web_command(self, **kwargs):
        if self.command == 'QUIT':
            return self.get_result_message(self.action.quit())

    class Factory:
        def create(self, **kwargs): return WebScraperComponentQuitCommand(**kwargs)
