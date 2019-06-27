from .command_processor import HitParadeBotCommandProcessor
class HitParadeBotOpenUrlProcessor(HitParadeBotCommandProcessor):
    """
    The class that allows a HitParadeBot to Open a Url before or during an operation.
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
        return self.open_url(id=self.scraper.get_id(), url=kwargs.get('url', None), caller=self.caller)

    class Factory:
        """
        Factory class used by the HitParadeBotCommandProcessorFactory
        """

        def create(self, **kwargs): return HitParadeBotOpenUrlProcessor(**kwargs)
