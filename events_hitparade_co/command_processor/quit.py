from .command_processor import HitParadeBotCommandProcessor
class HitParadeBotQuitCommandProcessor(HitParadeBotCommandProcessor):
    """
    This class allows a HitParadeBot to call the QuitCommand.
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
        return self.quit(id=self.scraper.get_id(), caller=self.caller)

    class Factory:
        """
        Factory class used by the HitParadeBotCommandProcessorFactory
        """
        def create(self, **kwargs): return HitParadeBotQuitCommandProcessor(**kwargs)
