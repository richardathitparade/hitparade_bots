from .command_processor import HitParadeBotCommandProcessor
class HitParadeBotScrapeProcessor(HitParadeBotCommandProcessor):
    """
    The class that allows a HitParadeBot to Scrape a URL before or during an operation.
    IS-A HitParadeBotCommandProcessor
    """
    def __init__(self, **kwargs):
        """
        Constructor
        :param kwargs: dict with data to process
        """
        super().__init__(**kwargs)

    def run_cmd(self, **kwargs):
        """
        run_cmd called by HitParadeBot
        :param kwargs: dict with data to process
        """
        return self.scrape_data(id=self.scraper.get_id(), scraping_props=self.bot_data, caller=self.caller)

    class Factory:
        """
        Factory class used by the HitParadeBotCommandProcessorFactory
        """

        def create(self, **kwargs):
            return HitParadeBotScrapeProcessor(**kwargs)
