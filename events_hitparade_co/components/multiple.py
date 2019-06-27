from events_hitparade_co.components.action import ScraperAction
class ScraperActionMultiple(ScraperAction):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.selectors = kwargs.get('selectors', [])
        self.scraper = kwargs.get('scraper', None)
        self.default_parser = kwargs.get('default_parser', None)


    def perform_action(self, **kwargs):
        """
        Performs action sepecified.

        :return: True if performed and False if not successfully performed
        """
        if not kwargs.get('driver', None) is None:
            self.driver = kwargs.get('driver', None)
        if self.command is None:
            self.command = 'SCRAPE'
        command_dict = {
            'action': self,
            'command': self.command
        }
        responses = []
        scraper_command = None
        new_command_dict = dict()
        new_command_dict.update(command_dict)
        new_command_dict['command'] = 'SCRAPE'
        for e in self.selectors:
            self.selector = e
            if scraper_command is None:
                scraper_command =  self.cache_output_component_func(**command_dict)
            response = scraper_command.execute_web_command(**command_dict)
            if response['success']:
                self.default_parser.reload_content()
                o,v = self.scraper.exec(**new_command_dict)
            responses.append(response)
        return responses
