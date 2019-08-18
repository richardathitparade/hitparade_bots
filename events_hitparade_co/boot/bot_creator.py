from events_hitparade_co.threads.consumer import HitParadeConsumerThread
from events_hitparade_co.bots.producer import HitParadeProducerBot
from events_hitparade_co.threads.transaction import HitParadeTransactionThread
from events_hitparade_co.messaging.messaging import MessagingQueue
from events_hitparade_co.threads.url_publisher import UrlPublisher


class HitParadeBotCreator:

    @staticmethod
    def bot(**bot_data):
        hit_parade_scrape_bot = None
        if bot_data['bot.type'] == 'consumer':
            hit_parade_scrape_bot = HitParadeConsumerThread(**bot_data)
        elif bot_data['bot.type'] == 'producer':
            hit_parade_scrape_bot = HitParadeProducerBot(**bot_data)
        elif bot_data['bot.type'] == 'transaction':
            hit_parade_scrape_bot = HitParadeTransactionThread(**bot_data)
        elif bot_data['bot.type'] == 'publisher':
            bot_data['publisher_id'] = MessagingQueue.unique_id(global_id=True, cache_manager=bot_data['cache_manager'])
            hit_parade_scrape_bot = UrlPublisher(**bot_data)
        else:
            print( 'Error bot type %s not recogized' % bot_data['bot.type']  )
        if not hit_parade_scrape_bot is None:
            print('starting bot...')
            hit_parade_scrape_bot.start()
