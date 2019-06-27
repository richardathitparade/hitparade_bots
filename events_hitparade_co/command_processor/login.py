from .command_processor import HitParadeBotCommandProcessor
class HitParadeBotLoginProcessor(HitParadeBotCommandProcessor):
    """
    The class that allows a HitParadeBot to Login if necessary that is if the login page is showing at any time during the scrape.
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
        return self.login(id=self.scraper.get_id(), selectors=kwargs.get('selectors', None), login_page_selectors=kwargs.get('login_page_selectors', None), login_button_selector=kwargs.get('login_button_selector', None), caller=self.caller)

    class Factory:
        """
        Factory class used by the HitParadeBotCommandProcessorFactory
        """
        def create(self, **kwargs): return HitParadeBotLoginProcessor(**kwargs)
