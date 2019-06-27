from events_hitparade_co.commands.command import WebScraperComponentCommand
class WebScraperComponentOpenLinksCommand(WebScraperComponentCommand):
    """
    Class to open a url.  Used by ScraperComponent through a factory method.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.link_set = kwargs.get('link_set', [])

    def execute_web_command(self, **kwargs):
        if self.command == 'OPEN_LINKS' and len(self.link_set) > 0:
            self.action.force_refresh = True
            self.action.scraper_url = self.link_set.pop()
            return self.get_result_message(self.action.open())
        else:
            self.action.finished = True
            return self.get_result_message(False)

    class Factory:
        def create(self, **kwargs): return WebScraperComponentOpenLinksCommand(**kwargs)
