from events_hitparade_co.commands.command import WebScraperComponentCommand
from events_hitparade_co.messaging.messaging import MessagingQueue
import time
import traceback
class WebScraperComponentClickScrapeCommand(WebScraperComponentCommand):
    """
    Class to open a url.  Used by ScraperComponent through a factory method.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.successful = False

    def is_successful(self):
        return self.successful

    def execute_web_command(self, **kwargs):
        if self.command == 'CLICK_SCRAPE':
            # return self.get_result_message(self.action.exec(**kwargs))
            el = self.action.selector
            for i in range(self.action.retry_count):
                if not el is None:
                    try:
                        if isinstance(self.action.selector, str):
                            try:
                                if self.action.wait_for:
                                    while el is None:
                                        el = self.action.driver.find_element_by_css_selector(self.action.selector)
                                        time.sleep(self.action.time_delay)
                                else:
                                    el = self.action.driver.find_element_by_css_selector(self.action.selector)
                            except:
                                traceback.print_exc()
                                pass
                            if not el is None:
                                el.click()
                                self.successful = True
                        else:
                            if not el is None:
                                el.click()
                                self.successful = True
                            else:
                                print('element passed in is null.')
                    except:
                        traceback.print_exc()
                        pass
        success_value = not el is None and self.is_successful()
        if success_value:
            MessagingQueue.send_msg_nowait(id=self.action.web_driver.id,direction='meta',cmd='SUCCESS', d={'selector' : self.action.selector}, caller='WebScraperComponentScrapeCommand')
        return self.get_result_message(success_value)

    class Factory:
        def create(self, **kwargs): return WebScraperComponentClickScrapeCommand(**kwargs)
