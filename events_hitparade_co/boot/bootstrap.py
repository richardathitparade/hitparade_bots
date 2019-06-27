import json
import os
from events_hitparade_co.factories.factories import HitParadeFactories
from events_hitparade_co.cache.cache_manager import CacheManager
from datetime import datetime
class HitParadeScrapingBootstrapper:
    class __HitParadeScrapingBootstrapper:
        def __init__(self, **kwargs):
            self.__dict__ = self.get_state_storage_stubs(dict_to_update=self.__dict__, cache_manager=None, **kwargs)
            json_data = None
            with open(self.config_file) as f:
                json_data = json.load(f)
                json_data = self.get_state_storage_stubs(dict_to_update=json_data, cache_manager=self.cache_manager, **kwargs)
                if json_data:
                    bot_counter = 1
                    bot_data = None
                    event_data = json_data.get( 'events', {} )
                    CacheManager.store_state_prop(prop='events', val=event_data)
                    json_data_selectors = json_data.get( 'data_selectors', [] )
                    self.url_generator_data = json_data.get( 'url_generators', [] )
                    self.url_republisher_data = json_data.get( 'url_republishers', [] )
                    self.global_variables['url_generator_data'] = self.url_generator_data
                    self.global_variables['url_generators'] = json_data.get( 'url_generators', [] )
                    json_data['republisher_data'] = self.url_republisher_data
                    json_data['url_republisher_data'] = self.url_republisher_data
                    self.get_state_storage_stubs(dict_to_update=json_data, cache_manager=self.cache_manager, **kwargs)


                    for republisher_data in json_data['url_republisher_data']:
                        republisher_data = self.get_state_storage_stubs(dict_to_update=republisher_data, cache_manager=self.cache_manager, **kwargs)
                        model_event = event_data.get(republisher_data.get('model_event_id' , None), None)
                        detail_model_event = event_data.get(republisher_data.get('detail_model_event_id' , None), None)
                        url_event = event_data.get(republisher_data.get('url_event_id' , None), None)
                        republisher_data['subscribable_event'] = {
                            'event' : model_event['publish_to'],
                            'recursive' : model_event['recursive'],
                            'append_pid' : model_event['append_pid']
                        }
                        republisher_data['publish_to_event'] = url_event['publish_to']
                        republisher_data['publish_to_model_event'] = detail_model_event['publish_to']
                        republisher_data['data_selector'] = detail_model_event.get('data_selector_id', None)
                        republisher_data['data_selector_id'] = detail_model_event.get('data_selector_id', None)
                        republisher_data['type_id'] = model_event.get('url_republisher', None)
                    self.global_variables['republisher_data'] = self.url_republisher_data
                    self.global_variables['url_republisher_data'] = self.url_republisher_data
                    ##data_selectors
                    for data_selector in json_data_selectors:
                        js_open_file = data_selector.get('path', None) + data_selector.get('file', None)
                        with open(js_open_file) as data_selector_file:
                            json_data_selector = json.load(data_selector_file)
                            json_data_selector['file'] = data_selector.get('file', None)
                            self.data_selectors[data_selector.get('id', None)] = json_data_selector
                            CacheManager.store_state_prop(prop=data_selector.get('id', None), val=json_data_selector)
                            #print('data selectors %s loaded ' % js_open_file)
                    #bots
                    for url_generator in self.url_generator_data:
                        url_event = event_data.get( url_generator.get('url_event_id', None), None )
                        url_generator['type_id'] = url_event.get('url_generator', None)
                        url_generator['data_selector'] = url_event.get('data_selector_id', None)
                        url_generator['data_selector_id'] = url_event.get('data_selector_id', None)
                        url_generator = self.get_state_storage_stubs(dict_to_update=url_generator, cache_manager=self.cache_manager, **kwargs)

                    while bot_counter == 1 or not bot_data is None and bot_data['active']:
                        bot_key = 'bot.' + str(bot_counter)
                        bot_data = json_data.get(bot_key, None)

                        if bot_data and bot_data['active']:
                            model_event = None if not bot_data.get('model_event_id', None) else event_data.get(bot_data.get('model_event_id', ''), None)
                            url_event = None if not bot_data.get('url_event_id', None) else event_data.get(bot_data.get('url_event_id', ''), None)
                            bot_data['command_processor'] = None if not model_event else model_event.get('command_processor', None)
                            bot_data['url_generator'] = None if not url_event else url_event.get('url_generator', None)
                            bot_data['url_republisher'] = None if not model_event else model_event.get('url_republisher', None)
                            bot_data = self.get_state_storage_stubs(dict_to_update=bot_data, cache_manager=self.cache_manager, **kwargs)
                            url_event_data = event_data.get(bot_data.get('url_event_id', ''), None)
                            model_event_data = event_data.get(bot_data.get('model_event_id', ''), None)
                            if not bot_data['bot.type'] == 'consumer':
                                bot_data['data_selector'] = url_event_data['data_selector_id']
                                bot_data['data_selector_id'] = url_event_data['data_selector_id']
                                bot_data['subscribable_event'] = {
                                    "event" : url_event_data['subscribe_to']  if bot_data['bot.type'] == 'publisher' else  url_event_data['publish_to'],
                                    "recursive": False,
                                    "append_pid": False if bot_data['bot.type'] == 'publisher' else url_event_data['append_pid']
                                }
                                bot_data["publish_event"] = url_event_data["publish_to"]  if bot_data['bot.type'] == 'publisher' else  url_event_data['subscribe_to']
                                bot_data["publish_to_event"] = model_event_data["publish_to"]
                        bot_counter += 1
                        if bot_data:
                            bot_data['cache_manager'] =  self.__dict__['cache_manager']
                            bot_data = self.get_state_storage_stubs(dict_to_update=bot_data, **json_data)
                            for k in json_data.keys():
                                if json_data.get(k,None) and isinstance(json_data.get(k,None), str):
                                    print('json_data -- %s => %s ' % ( k , json_data.get(k, None) ) )
                            bot_data['cache_input_file'] =  self.__dict__['cache_input_file']
                            bot_data['scraper_type'] =  self.__dict__['scraper_type']
                            bot_data['chrome_binary'] =  self.__dict__['chrome_binary']
                            bot_data['scraper_type_parser'] =  self.__dict__['scraper_type_parser']
                            for k in bot_data.get('global_variables', {}).keys():
                                if bot_data.get(k,None) and isinstance(bot_data.get(k,None), str):
                                    print('bot_data -- %s => %s ' % ( k , bot_data.get(k, None) ) )
                            bot_data['default_parser'] = HitParadeFactories.create(  type_id=json_data.get('scraper_type_parser', None), **json_data )
                            self.bot_data_dictionary.append(bot_data)
            print('environment path for these bots is %s' % os.environ['PATH'])


        def get_state_storage_stubs(self, dict_to_update=None,cache_manager=None, **kwargs):
            bot_data = dict() 
            bot_data['config_file'] = kwargs.get('config_file', './events_hitparade_co/config/config.json')
            bot_data['cache_output_component_func'] = HitParadeFactories.create
            kwargs['cache_output_component_func'] = HitParadeFactories.create
            bot_data['cache_manager'] = CacheManager.get_instance(**kwargs) if cache_manager is None else cache_manager
            bot_data['cache_output_component_func'] = HitParadeFactories.create
            bot_data['state_storage_get_prop'] = CacheManager.get_state_prop
            bot_data['state_storage_store_prop'] = CacheManager.store_state_prop
            bot_data['state_storage_globals'] = CacheManager.globals
            bot_data['state_storage_increment_val'] = CacheManager.increment_val
            bot_data['state_storage_append_val'] = CacheManager.append_val
            bot_data['state_storage_decrement_val'] = CacheManager.decrement_val
            bot_data['state_storage_delete_prop'] = CacheManager.delete_state_prop
            bot_data['state_storage_divide_val'] = CacheManager.divide_val
            bot_data['state_storage_multiply_val'] = CacheManager.multiply_val
            global_variables = CacheManager.globals()
            #print('globals are %s ' % json.dumps(global_variables))
            evals = global_variables.get('evals', {})
            for k in evals.keys():
                global_variables[k] = eval(evals.get(k))
                CacheManager.store_state_prop(prop=k, val=global_variables[k])
            for k in global_variables.keys():
                CacheManager.store_state_prop(prop=k, val=global_variables[k])
            bot_data['global_variables'] = global_variables
            bot_data['evals'] = evals

            dict_to_update = dict(list(bot_data.items()) + list(dict_to_update.items()))
            dict_to_update = dict(list(global_variables.items()) + list(dict_to_update.items()))
            dict_to_update = dict(list(evals.items()) + list(dict_to_update.items()))
            return dict_to_update

        def get_url_generator(self, type_id=None):
            return self.url_generators.get(type_id, None)

        def get_url_republisher(self, type_id=None):
            return self.url_republishers.get(type_id, None)

        def get_bot_data(self):
            return self.bot_data_dictionary

        def get_data_selectors(self, id=None):
            return self.data_selectors.get(id, None) if id else self.data_selectors



    instance = None

    def __init__(self, **kwargs):
        if HitParadeScrapingBootstrapper.instance is None:
            HitParadeScrapingBootstrapper.instance = HitParadeScrapingBootstrapper.__HitParadeScrapingBootstrapper(**kwargs)

    @staticmethod
    def get_data_selectors(id=None):
        return HitParadeScrapingBootstrapper.instance.get_data_selectors(id=id)

    @staticmethod
    def get_url_generator(type_id=None):
        gen_data = HitParadeScrapingBootstrapper.instance.url_generator_data
        generators = list(filter(lambda x:  x.get('type_id', None) and x.get('type_id', None) == type_id, gen_data))
        if generators and isinstance(generators, list) and len(generators) > 0:
            return generators[0]
        return None

    @staticmethod
    def get_url_republisher(type_id=None):
        republisher_data = HitParadeScrapingBootstrapper.instance.url_republisher_data
        republishers =  list(filter(lambda x:  x.get('type_id', None) and x.get('type_id', None) == type_id, republisher_data))
        if republishers and isinstance(republishers, list) and len(republishers) > 0:
            return republishers[0]
        return None

    @staticmethod
    def boot_up(**kwargs):
        if HitParadeScrapingBootstrapper.instance is None:
            HitParadeScrapingBootstrapper(**kwargs)
        return HitParadeScrapingBootstrapper.instance

    @staticmethod
    def get_bots(**kwargs):
        return HitParadeScrapingBootstrapper.boot_up(**kwargs).get_bot_data()
