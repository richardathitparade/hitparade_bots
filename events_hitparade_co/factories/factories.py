from events_hitparade_co.parsers.beautiful_soup import BeautifulSoupParser
from events_hitparade_co.scrapers.chrome import ChromeWebScraper
from events_hitparade_co.scrapers.firefox import FirefoxWebScraper
from events_hitparade_co.command_processor.login import HitParadeBotLoginProcessor
from events_hitparade_co.command_processor.open import HitParadeBotOpenUrlProcessor
from events_hitparade_co.command_processor.quit import HitParadeBotQuitCommandProcessor
from events_hitparade_co.command_processor.scrape import HitParadeBotScrapeProcessor
from events_hitparade_co.output.cache import HitParadeCachePublisherOuput
from events_hitparade_co.output.default import HitParadeDefaultOuput
from events_hitparade_co.messaging.messaging import MessagingQueue
from events_hitparade_co.components.scraper import Scraper
from events_hitparade_co.components.action import ScraperAction
from events_hitparade_co.components.login import ScraperLogin
from events_hitparade_co.parsers.selenium import SeleniumParser
from events_hitparade_co.url_generators.generator import UrlGenerator 

class HitParadeFactories:
    factory_command_mapping = {'TODAY': 'TodaySegmentProcessor'}
    FACTORIES = {}
    command_mapping = {
        'QUIT': 'WebScraperComponentQuitCommand',
        'OPEN': 'WebScraperComponentOpenCommand',
        'SCRAPE': 'WebScraperComponentScrapeCommand',
        'OPEN_LINKS': 'WebScraperComponentOpenLinksCommand',
        'CLICK_SCRAPE': 'WebScraperComponentClickScrapeCommand'
    }

    @staticmethod
    def create( type_id=None,**kwargs ):
        kwargs['unique_id'] = MessagingQueue.unique_id(global_id=True,cache_manager=kwargs.get('cache_manager', None))
        command_value = kwargs.get('command' , None )
        nocommand = kwargs.get('nocommand' , False )
        if command_value is None or nocommand:
            if not kwargs.get('default_parser', None) is None and isinstance(kwargs.get('default_parser', None), str):
                kwargs['default_parser'] = HitParadeFactories.create(kwargs.get('default_parser', 'BeautifulSoupParser'), **kwargs)
            elif type_id is not None and HitParadeFactories.factory_command_mapping.get(type_id.upper().strip(), None) is not None and HitParadeFactories.FACTORIES.get(type_id, None) is None:
                HitParadeFactories.importit(HitParadeFactories.factory_command_mapping.get(type_id.upper().strip(), None))
                HitParadeFactories.FACTORIES[type_id] = eval(HitParadeFactories.factory_command_mapping.get(type_id.upper().strip(), None) + '.Factory()')
                HitParadeFactories.FACTORIES[HitParadeFactories.factory_command_mapping.get(type_id.upper().strip(), None)] = eval(HitParadeFactories.factory_command_mapping.get(type_id.upper().strip(), None) + '.Factory()')
            elif HitParadeFactories.FACTORIES.get( type_id, None ) is None:
                print('typeid is %s ' %type_id)
                HitParadeFactories.FACTORIES[type_id] = eval( type_id +'.Factory()' )
            return HitParadeFactories.FACTORIES.get(type_id, None).create(**kwargs)
        else:
            if not command_value is None and not kwargs.get('command', None) in HitParadeFactories.FACTORIES:
                HitParadeFactories.FACTORIES[command_value] = eval(  HitParadeFactories.command_mapping.get(command_value, None) + '.Factory()')
            return HitParadeFactories.FACTORIES[command_value].create(**kwargs)
