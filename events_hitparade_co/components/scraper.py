from events_hitparade_co.bots.bot import HitParadeBot
import traceback
from events_hitparade_co.components.component import ScraperComponent
from events_hitparade_co.parsers.beautiful_soup import BeautifulSoupParser
import time
class Scraper(ScraperComponent):
    """
    Class that if necessary logs and then scrapes data.
    When it completes the thread will complete.
    This scraper could scrape continually or just one time.
    """

    def __init__(self, **kwargs):
        """
        Constructor for all ScraperComponents
        :param **kwargs dict using the following data.
                driver:  WebScraper driver default None
                web_driver: WebScraper  default None
                scraper_url: str url to open default None
                open_url: bool True to open upon startup and False it waits.
                command:  str command this component responds to and runs.
                default_parser: HitParadeParser defaults to BeautifulSoupParser
                retry_count: int number of times to retry anything default HitParadeBot.DEFAULT_RETRY
                ** scraper_logins: Login components that check if they are on the login page and login.
                ** selectors: Selectors to scrape
                ** sleep_time time to sleep between scrapes if and only if residual defaults to 5 seconds
        """
        super().__init__( **kwargs )
        self.scraper_logins = list( map(lambda l: self.cache_manager.cache_output_component_func('ScraperLogin',**l), kwargs.get('scraper_logins', [])))
        self.scraper_prerun_actions = list(map(lambda l: self.cache_manager.cache_output_component_func('ScraperAction',**l) , kwargs.get('scraper_prerun_actions', [])))
        self.scraper_postrun_actions = list(map(lambda l: self.cache_manager.cache_output_component_func('ScraperAction',**l) , kwargs.get('scraper_postrun_actions', [])))
        #self.data_selectors = self.state_storage_get_prop('data_selectors').get('data_selectors', {})
        # if isinstance(self.data_selectors, dict):
        #     self.data_selectors = [self.data_selectors]
        self.sleep_time = kwargs.get('sleep_time', HitParadeBot.DEFAULT_RETRY)
        self.force_refresh = self.state_storage_get_prop('data_selectors').get('force_refresh', False) if not kwargs.get('data_selectors', {}) is None else False
        self.default_parser =  kwargs.get('default_parser', None)                                # BeautifulSoupParser(**kwargs)
        #if self.state_storage_get_prop('scraper_url') is None and not self.state_storage_get_prop('scraper_url') is None:  #self.data_selectors.get('scraper_url', None) is None:
        self.scraper_url = self.state_storage_get_prop('scraper_url') #self.data_selectors.get('scraper_url', None)
        self.login_if_necessary(**kwargs)
        self.start_scrape_iteration = 1
        self.end_scrape_iteration = 10
        self.current_scrape_iteration = 1
        self.wait_for_element = self.state_storage_get_prop('data_selectors').get('wait_for_element', None) if not self.state_storage_get_prop('data_selectors') is None else None
        self.wait_for_element_multiple = self.state_storage_get_prop('data_selectors').get('wait_for_element.multiple', False) if not self.state_storage_get_prop('data_selectors') is None else False
        self.wait_for_element_sleep_time = self.state_storage_get_prop('data_selectors').get('wait_for_element.sleep_time', 2) if not self.state_storage_get_prop('data_selectors') is None else 2
        self.wait_for_element_time_out = self.state_storage_get_prop('data_selectors').get('wait_for_element.time_out', 20) if not self.state_storage_get_prop('data_selectors') is None else 20
        self.wait_for_element_reload_content = self.state_storage_get_prop('data_selectors').get('wait_for_element.reload_content', True) if not self.state_storage_get_prop('data_selectors') is None else True
        self.run_postrun_actions_to_reset = kwargs.get('run_postrun_actions_to_reset', True)

    def login_if_necessary(self, **kwargs):
        """
        Logs the scraper in if it is necessary.
        :param kwargs: dict data if necessary
        :return:
        """
        try:
            self.open()
            if not self.scraper_logins is None:
                for login_val in self.scraper_logins:
                    if login_val.is_on_login_page():
                        login_kwargs = dict()
                        login_kwargs.update(kwargs)
                        login_kwargs['web_driver'] = self.web_driver
                        login_val.login_if_necessary(**login_kwargs)
        except:
            traceback.print_exc()
            print('error opening scrape url %s ' % self.scraper_url)

    def wait_for_component(self):
        total_sleeptime = 0
        element_length = 0
        while total_sleeptime < self.wait_for_element_time_out and element_length == 0:
            if self.wait_for_element_reload_content:
                self.default_parser.reload_content()
            elements = self.default_parser.get_elements( css_selector=self.wait_for_element, sub_element=None, is_multiple=self.wait_for_element_multiple, log_exceptions=False)
            if not elements is None:
                element_length = len(elements)
            if element_length == 0:
                time.sleep(self.wait_for_element_sleep_time)
            total_sleeptime += self.wait_for_element_sleep_time
        print('found %s length %s in %s '%(str(self.wait_for_element),str(element_length),str(total_sleeptime)))


    def scrape_basic_selector(self,selector=None, sub_element=None,attributes=None,scrape_types=None,iframe_index=0 ,log_exceptions=True):
        """
        Returns an atomic element and all properties requested.
        :param selector: str selector
        :param sub_element: element sub_element to choose from. Or not if it is None
        :param attributes: list[] of attributes to choose.
        :param scrape_types: list[] types of infromation to choose.
        :param iframe_index: int index of iframe to use if necessary.
        :param log_exceptions: bool True to print exceptiosn False to not print exceptions.
        :return: dict values selected.  Empty dict if nothing is found.
        """
        selector_vals = dict()
        if len(attributes) > 0 and not 'attributes' in scrape_types:
            scrape_types.append('attributes')
        try:
            selector_vals[selector] = self.default_parser.get_atomic_element(scrape_types=scrape_types,
                                                                                          selector=selector,
                                                                                          attributes=attributes,
                                                                                          sub_element=sub_element,
                                                                                          iframe_index=iframe_index,
                                                                                          log_exceptions=log_exceptions)
        except:
            if log_exceptions:
                traceback.print_exc()
        return selector_vals

    def extract_to_attributes(self,
                                selector=None,
                                prefix=None,
                                data_selector_dict=None,
                                selector_separator='>',
                                default_selectors=[],
                                default_attributes=[],
                                default_multiple=False,
                                default_types=['text']):
        selector_key = selector# if prefix is None else prefix + selector_separator + selector
        internal_selectors = data_selector_dict.get(selector + '.selectors', default_selectors)
        iframe_properties =  data_selector_dict.get(selector + '.iframe', {})
        attributes = data_selector_dict.get(selector + '.attributes', default_attributes)
        scrape_types = data_selector_dict.get(selector + '.types', default_types)
        selector_multiple = data_selector_dict.get(selector + '.multiple', default_multiple)
        if selector_multiple and len(scrape_types) == 0:
            if not 'elements' in scrape_types:
                scrape_types.append('elements')
        return selector_key, internal_selectors, attributes, scrape_types, iframe_properties, selector_multiple


    def reformat_selector(self, card_item_listing=None, filtered_props=['message']):
        card_dict = dict()
        for card_item_props in card_item_listing:
            for k,v in card_item_props.items():
                                        if isinstance(v, dict):
                                            for k1,v1 in v.items():
                                                if isinstance(v1, dict) and len(v1.items()) > 0:
                                                    card_dict[k] = v1
                                                    card_dict[k1] = v1
                                                else:
                                                    card_dict[k] = v1
                                                    card_dict[k1] = v1
                                        elif not k in filtered_props:
                                            if not v is None and isinstance(v, dict) and len(v.items()) > 0:
                                                card_dict[k] = v
        return card_dict


    def scrape_one_element(self, selector=None, sub_element=None, attributes=None, scrape_types=None, iframe_index=0, log_exceptions=False ):
        scrape_property = dict()
        try:
            scrape_property = self.scrape_basic_selector(selector=selector, sub_element=sub_element,
                                                                    attributes=attributes, scrape_types=scrape_types,
                                                                    iframe_index=iframe_index )
        except:
            if log_exceptions:
                traceback.print_exc()
        return scrape_property

    def scrape_nested_selectors(self,prefix=None,selector=None,data_selector_dict=None, sub_element=None, scrape_types=['text'], log_exceptions=True):
        def add_new_property(aggregate_dict, new_dict, s):
            if not aggregate_dict is None:
                if not new_dict is None and len(new_dict.items()) > 0:
                    v = new_dict.get(s,None)
                    if not v is None:
                        if isinstance(v, dict):
                            if len(v.items()) > 0:
                                aggregate_dict.update(new_dict)
                        else:
                            aggregate_dict.update(new_dict)
            return aggregate_dict

        scraped_data = dict()
        selector_key, internal_selectors,   attributes, scrape_types_, iframe_properties, is_multiple = self.extract_to_attributes(selector=selector, prefix=prefix, data_selector_dict=data_selector_dict,default_types=scrape_types, default_multiple=False)
        all_elements = []
        if is_multiple:
            counter = 0
            while counter < 3 and (all_elements is None or len(all_elements) == 0):
                counter += 1
                all_elements = self.default_parser.get_atomic_element(scrape_types=scrape_types_,
                                                       selector=selector_key,
                                                       attributes=attributes,
                                                       iframe_index=iframe_properties.get('index', 0),
                                                       sub_element=sub_element,
                                                       is_multiple=is_multiple,
                                                       log_exceptions=log_exceptions)

            if not all_elements is None and not all_elements.get('element', None) is None and len(all_elements.get('element', [])) > 0:
                element_data = []
                for idx, root_element in enumerate( all_elements.get('element', []) ):
                    node_properties = dict()
                    for i, k in enumerate(all_elements.keys()):
                        if not k == 'element':
                            try:
                                node_properties[k] = all_elements[k][idx]
                            except:
                                if log_exceptions:
                                    print(' node_properties exception k=%s, idx=%s ' % (str(k), str(idx)))
                                    traceback.print_exc()
                    scrape_types_ = [x for x in scrape_types_ if not x == 'elements']
                    if not internal_selectors is None and len(internal_selectors) > 0:
                        for internal_selector in internal_selectors:
                            sub_element_scraped_data = self.scrape_nested_selectors(prefix=selector,
                                                                                    selector=internal_selector,
                                                                                    data_selector_dict=data_selector_dict,
                                                                                    scrape_types=scrape_types_,
                                                                                    sub_element=root_element)
                            if not sub_element_scraped_data is None and sub_element_scraped_data.get(internal_selector, None) is None:
                                new_dict = {
                                    internal_selector: sub_element_scraped_data
                                }
                                sub_element_scraped_data = new_dict

                            node_properties = add_new_property(node_properties, sub_element_scraped_data, internal_selector)
                        if not node_properties is None and len(node_properties.items()) > 0:
                            element_data.append(node_properties)
                    else:
                        select_one_element_data = self.scrape_one_element(selector=selector, sub_element=sub_element, attributes=attributes,
                                                    scrape_types=scrape_types_,
                                                    iframe_index=iframe_properties.get('index', 0),
                                                    log_exceptions=log_exceptions)
                        scraped_data = add_new_property(scraped_data, select_one_element_data, selector)
                if len(element_data) > 0:
                    scraped_data[selector] = element_data
            elif not all_elements is None and len(all_elements.get('text', [])) > 0:
                all_elements[selector] = all_elements.get('text', [])
                del all_elements['text']
                scraped_data = add_new_property(scraped_data, all_elements, selector)
            else:
                if log_exceptions:
                    print(' Error selector %s did not have elements on this page. ' % selector)
        else:
            if len(internal_selectors) == 0:
                internal_selector_data = self.scrape_one_element(selector=selector, sub_element=sub_element,attributes=attributes, scrape_types=scrape_types_, iframe_index=iframe_properties.get('index',0), log_exceptions=log_exceptions)
                scraped_data = add_new_property(scraped_data, internal_selector_data, selector)
            else:
                if not internal_selectors is None and len(internal_selectors) > 0:
                    new_sub_element = self.scrape_one_element(selector=selector, sub_element=sub_element,attributes=attributes, scrape_types=scrape_types_, iframe_index=iframe_properties.get('index',0), log_exceptions=log_exceptions)
                    for internal_selector in internal_selectors:
                        sub_element_scraped_data = self.scrape_nested_selectors(prefix=selector,
                                                                                selector=internal_selector,
                                                                                data_selector_dict=data_selector_dict,
                                                                                scrape_types=scrape_types_,
                                                                                sub_element=new_sub_element.get(selector, {}).get('element', None))
                        scraped_data = add_new_property(scraped_data, sub_element_scraped_data, internal_selector)
        return scraped_data

    def run_actions_and_remove(self,actions=[], **kwargs):
        if actions:
            for action in actions:
                s = self.run_action(action=action, **kwargs).get('success', False)
                if s and action.use_once:
                    action.finished = True
                if not s and action.use_until_failure:
                    action.finished = True
        return list(filter(lambda a: not a.finished, actions))

    def run_prerun_actions(self, **kwargs):
        return self.run_actions_and_remove(actions=self.scraper_prerun_actions,**kwargs)

    def run_postrun_actions(self, **kwargs):
        self.scraper_postrun_actions =  self.run_actions_and_remove(actions=self.scraper_postrun_actions, **kwargs)
        return self.scraper_postrun_actions


    def run_action(self, action=None, **kwargs):
        try:
            return action.exec(**kwargs)
        except:
            traceback.print_exc()

    def should_run_reset(self):
        return self.run_postrun_actions_to_reset and self.current_scrape_iteration > self.start_scrape_iteration and len(self.scraper_postrun_actions) > 0

    def update_postrun_iteration(self, **kwargs):
        pass

    def scrape_data(self, **kwargs):
        """
        Override this if you want to scrape differently.
        This is just generic
        :return:
        """
        while self.should_run_reset():
            self.run_postrun_actions(**kwargs)
            self.update_postrun_iteration(**kwargs)

        scraped_data = dict()
        # if kwargs.get('response_queue', None) is None:
        #     scraped_data['command'] = 'SCRAPE'
        self.login_if_necessary()
        self.scraper_prerun_actions = self.run_prerun_actions(**kwargs)
        if not self.state_storage_get_prop('data_selectors') is None:
            self.default_parser.reload_content()
            scraped_data['current_url'] = self.driver.current_url
            for data_selector_dict in self.state_storage_get_prop('data_selectors').get('data_selectors', []): #self.data_selectors:
                if not data_selector_dict.get('scraper_url', None) is None and not data_selector_dict.get('scraper_url', None) ==self.scraper_url:
                    self.scraper_url = data_selector_dict['scraper_url']
                    print('scrape_data --> open  ' )
                    self.open()

                for i,selector in enumerate(data_selector_dict.get('selectors', [])):
                    if i == 0:
                        print('waiting for %s ' % selector)
                        self.wait_for_component()
                        try:
                            scraped_data_dict = self.scrape_nested_selectors(prefix=None,
                                                                                          selector=selector,
                                                                                          data_selector_dict=data_selector_dict,
                                                                                          sub_element=None)
                            if not scraped_data_dict is None and isinstance(scraped_data_dict, dict):
                                scraped_data.update(scraped_data_dict)
                            else:
                                scraped_data[selector] = scraped_data_dict
                        except:
                            traceback.print_exc()

        for k in scraped_data:
            if not scraped_data.get(k,None) is None and isinstance(scraped_data.get(k,None) , dict):
                d = scraped_data.get(k,None)
                remove_keys = []
                for k1 in d:
                    if not isinstance(d.get(k1, None), str):
                        remove_keys.append(k1)
                for rk in remove_keys:
                    d.pop(rk,None)
        self.scraper_postrun_actions = self.run_postrun_actions(**kwargs)
        return scraped_data

    def exec(self,**kwargs):
        scraper_vals = self.scrape_data(**kwargs)
        self.respond(scraper_vals)
        return scraper_vals

    class Factory:
        def create(self, **kwargs):
            scraper_kwargs = dict()
            scraper_kwargs.update(kwargs)
            scraper_kwargs['scraper_url'] = kwargs.get('state_storage_get_prop', None)('scraper_url')
            logins = kwargs.get('scraper_logins',[])
            scraper_logins = []
            for login in logins:
                login['driver'] = kwargs.get('driver', None)
                login['web_driver'] = kwargs.get('web_driver', None)
                login['scraper_url'] = kwargs.get('state_storage_get_prop', None)('scraper_url')
            scraper_prerun_actions =   kwargs.get('state_storage_get_prop', None)('data_selectors').get('scraper_prerun_actions', [])
            scraper_postrun_actions =  kwargs.get('state_storage_get_prop', None)('data_selectors').get('scraper_postrun_actions', [])
            for a in scraper_prerun_actions:
                a['driver'] = kwargs.get('driver', None)
                a['web_driver'] = kwargs.get('web_driver', None)
            for a in scraper_postrun_actions:
                a['driver'] = kwargs.get('driver', None)
                a['web_driver'] = kwargs.get('web_driver', None)
            scraper_kwargs['scraper_logins'] = scraper_logins
            scraper_kwargs['scraper_prerun_actions'] = kwargs.get('state_storage_get_prop', None)('data_selectors').get('scraper_prerun_actions', [])
            scraper_kwargs['scraper_postrun_actions'] = kwargs.get('state_storage_get_prop', None)('data_selectors').get('scraper_postrun_actions', [])
            for k in kwargs:
                if 'storage_' in k:
                    scraper_kwargs[k] = kwargs[k]
            return Scraper(**scraper_kwargs)
