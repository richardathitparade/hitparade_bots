import pandas as pd
from events_hitparade_co.cache.redis_cache import RedisCache
from events_hitparade_co.cache.subscriber import CacheSubscriber
import requests
import traceback
import json
from datetime import datetime
class CacheEventLookup:
    def __init__(self, **kwargs):
        self.ip = self.get_external_ip_addresss()
        self.cache_configs =  kwargs.get('cache_configs', [])

    def get_external_ip_addresss(self):
        try:
            return CacheManager.get_external_ip_addresss()
        except:
            print('error getting ip address ')
            traceback.print_exc()

    def get_cache(self, event=None):
        return self.__find_c_prop(p='event', v=event)

    def __find_c_prop(self, p=None, v=None,add_subscribed=True):
        try:
            event_cache = []
            if add_subscribed:
                event_cache = list(filter(lambda c: c[p] == v, self.get_subscribed()))
            else:
                event_cache = list(filter( lambda c: not c['cache_type'] == 'broker' and c.get(p, None) == v, self.cache_configs))
            if len(event_cache) > 0:
                return event_cache[0]
            return None
        except:
            return None

    def get_subscribed(self):
        return list(filter(lambda c: not c.get('cache_type', None) == 'broker' and c.get('subscribed', False), self.cache_configs))

    def get_unsubscribed(self):
        return list(filter(lambda c: not c.get('cache_type', None) == 'broker' and not c.get('subscribed', False), self.cache_configs))

    def get_cache_to_subscribe(self, event=None,pid=None):
        cache = self.get_cache(event=event)
        if cache and len(cache) > 0:
            return cache[0], False
        else:
            unused_caches = self.get_unsubscribed()
            if len(unused_caches) == 0:
                return None, False
            else:
                used_cache = unused_caches[0]
                used_cache['subscribed'] = True
                used_cache['event'] = event
                used_cache['pid'] = pid
                used_cache['ip'] = self.ip
                return used_cache, True

    def get_cached_vals(self):
        subscribed_caches = self.get_subscribed()
        event_mapping = dict()
        pid_mapping = dict()
        ip_mapping = {
            self.ip : list( map( lambda c: c['event'], subscribed_caches ) ),
            self.ip+'.pid': list(map(lambda c: c['pid'], subscribed_caches))
        }
        for c in subscribed_caches:
            if isinstance(c.get('event', None), list):
                for _c in c.get('event', None):
                    event_mapping[_c] = c
                    ip_mapping[c.get('ip', None)] = c.get('event', None)
                    pid_mapping[c.get('pid', None)] = c.get('ip', None)
            else:
                event_mapping[c.get('event', None)] = c
                ip_mapping[c.get('ip', None)] = c.get('event', None)
                pid_mapping[c.get('pid', None)] = c.get('ip', None)
        return event_mapping, ip_mapping, pid_mapping


class CacheManager:

    class __CacheManager:
        def __init__(self, **kwargs):
            self.cache_index = dict()
            self.cache_output_component_func = kwargs.get('cache_output_component_func', None)
            self.cache_index['cache_listing'] = []
            self.cache_index['event_to_process'] = dict()
            self.cache_index['process_to_event'] = dict()
            self.unique_ids_key = 'unique_ids'
            input_file = kwargs.get('input_file', kwargs.get('cache_input_file', './events_hitparade_co/config/cache.csv') )
            df = pd.read_csv(input_file)
            self.cache_index['metadata'] = {'recursive' : kwargs.get('recursive', False), 'events' : kwargs.get('events', None)}
            for idx,row in df.iterrows():
                new_cache_to = dict()
                new_cache_to['host'] = row['host']
                new_cache_to['port'] = int(row['port'])
                new_cache_to['cache_type'] = row['cache_type']
                new_cache_to['clear'] = row['clear']
                new_cache = RedisCache(**new_cache_to)
                self.cache_index['cache_listing'].append(new_cache)
                key_value = new_cache_to['host'] + '.' + str(new_cache_to['port'])
                #index both host name / port AND cache type
                self.cache_index[key_value] = new_cache
                self.cache_index[new_cache_to['cache_type']] = new_cache
                publisher_key_host_port = key_value+'.publisher'
                publisher_key_cache_type = new_cache_to['cache_type'] + '.publisher'
                cache_publisher = new_cache.pubsub()
                self.cache_index[publisher_key_host_port] = cache_publisher
                self.cache_index[publisher_key_cache_type] = cache_publisher
                print('added cahce %s :: %s  with key %s ' % (new_cache_to['host'], str(new_cache_to['port']), key_value))
            self.cache_broker_config = dict()
            self.cache_broker_config['configs'] = list(map(lambda c : {'host' : c.host, 'port' : c.port, 'cache_type' : c.cache_type}, self.cache_index['cache_listing']))
            self.cache_broker_config['process_to_event'] = dict()
            self.cache_broker_config['event_to_process'] = dict()
            self.get_broker().set( 'cache_config', self.cache_broker_config )
            cache_lookup_kwargs = {
                'cache_configs' :  self.cache_broker_config['configs']
            }
            self.cache_tables = CacheEventLookup(**cache_lookup_kwargs)
            self.__init = False
            self.ip = CacheManager.get_external_ip_addresss()
            print('ip address is %s ' % self.ip)

            try:
                self.state_config_path = kwargs.get('state_config_path', './events_hitparade_co/config/' )
                self.state_config = kwargs.get('state_config', 'storage_state.json')
                state_config_file = self.state_config_path + self.state_config
                with open(state_config_file) as f:
                    json_data = json.load(f)
                    ip_properties_dict = dict()
                    ip_properties_dict.update(json_data)
                    self.get_broker().set(self.ip, ip_properties_dict)
            except:
                print('exception reading globals')
                traceback.print_exc()

        def init_caches(self, **kwargs):
            if not self.__init:
                if self.cache_output_component_func:
                    for idx, cache_object in enumerate(self.cache_index['cache_listing']):
                        output_kwargs = dict()
                        key_value = cache_object.host + '.' + str(cache_object.port)
                        output_key_host_port = key_value + '.output'
                        output_key_cache_type =  cache_object.cache_type  + '.output'
                        output_kwargs['type'] = 'cache'
                        output_kwargs['cache'] = cache_object
                        output_kwargs['cache_manager'] = kwargs.get('cache_manager', None)
                        output_kwargs['recursive'] =  self.cache_index['metadata'].get('recursive', False)
                        output_kwargs['events'] = self.cache_index['metadata'].get('events', [])
                        cache_output_compotnent = self.cache_output_component_func(type_id='HitParadeCachePublisherOuput', **output_kwargs)
                        self.cache_index[output_key_host_port] = cache_output_compotnent
                        self.cache_index[output_key_cache_type] = cache_output_compotnent
                        self.ip = requests.get('http://ip.42.pl/raw').text
                        self.__init = True
                else:
                    print('error , no factory create function created.')

        def get_cache(self, host=None, port=None, cache_type=None):
            if host and port:
                new_key = host + '.' + str(port)
                cache_value = self.cache_index.get(new_key, None)
                if cache_value is None:
                    new_kwargs = dict()
                    new_kwargs['host'] = host
                    new_kwargs['port'] = port
                    cache_value = RedisCache(**new_kwargs)
                    self.cache_index[new_key] = cache_value
                return cache_value
            elif cache_type:
                cache_value = self.cache_index.get(cache_type, None)
                if not cache_value:
                    print("Error:: Cache Value is not found for cache_type %s " % cache_type)
                return cache_value
            else:
                print("Error, host,port and cache type are not defined therefore we cannot find the cache.")
                return None

        def get_output(self, host=None, port=None, cache_type=None, output=True):
            if host and port:
                if output:
                    new_key = host + '.' + str(port) + '.output'
                else:
                    new_key = host + '.' + str(port)
                cache_value = self.cache_index.get(new_key, None)
                if cache_value is None:
                    new_kwargs = dict()
                    new_kwargs['host'] = host
                    new_kwargs['port'] = port
                    cache_value = RedisCache(**new_kwargs)
                    self.cache_index[new_key] = cache_value
                return cache_value
            elif cache_type:
                if output:
                    new_cache_type_key = cache_type + '.output'
                else:
                    new_cache_type_key = cache_type
                cache_value = self.cache_index.get(new_cache_type_key, None)
                if not cache_value:
                    print("Error:: Output Value is not found for cache_type %s - %s" % (new_cache_type_key, cache_type))
                return cache_value
            else:
                print("Error, host,port and cache type are not defined therefore we cannot find the cache.")
                return None

        def get_publisher(self, host=None, port=None, cache_type=None):
            if host and port:
                new_key = host + '.' + str(port) + '.publisher'
                cache_value = self.cache_index.get(new_key, None)
                if cache_value is None:
                    new_kwargs = dict()
                    new_kwargs['host'] = host
                    new_kwargs['port'] = port
                    cache_value = RedisCache(**new_kwargs)
                    self.cache_index[new_key] = cache_value
                return cache_value
            elif cache_type:
                new_cache_type_key = cache_type + '.publisher'
                cache_value = self.cache_index.get(new_cache_type_key, None)
                if not cache_value:
                    print( "Error:: Publisher Value is not found for cache_type %s - %s" % (new_cache_type_key, cache_type) )
                return cache_value
            else:
                print("Error, host,port and cache type are not defined therefore we cannot find the cache.")
                return None

        def get_broker(self):
            return self.get_output(cache_type='broker', output=False)

        def __merge(self, cached_regristries=None, key=None, mapping=None):
            if cached_regristries.get(key, None) is None:
                cached_regristries[key] = mapping
            else:
                old_mapping = json.loads(cached_regristries[key])
                old_mapping.update(mapping)
                cached_regristries[key] = old_mapping

        def find_cache(self, event=None, process_id=None, action='SUBSCRIBE'):
                full_event_cache = self.cache_tables.get_cache(event=event)
                if action == 'SUBSCRIBE':
                    if full_event_cache is None:
                        full_event_cache, new_cache = self.cache_tables.get_cache_to_subscribe(event=event, pid=process_id)
                cached_regristries = {
                    'EVENT_REGISTRY': self.get_broker().get('EVENT_REGISTRY'),
                    'PID_REGISTRY': self.get_broker().get('PID_REGISTRY'),
                    'IP_REGISTRY': self.get_broker().get('IP_REGISTRY'),
                }
                if action == 'PUBLISH':
                    registry_data = json.loads(cached_regristries.get('EVENT_REGISTRY', None)) if cached_regristries.get(
                        'EVENT_REGISTRY', None) else {}
                    full_event_cache = registry_data.get(event, None)

                # saving --
                event_mapping, ip_mapping, pid_mapping = self.cache_tables.get_cached_vals()
                self.__merge(cached_regristries=cached_regristries, key='EVENT_REGISTRY', mapping=event_mapping)
                self.__merge(cached_regristries=cached_regristries, key='PID_REGISTRY', mapping=pid_mapping)
                self.__merge(cached_regristries=cached_regristries, key='IP_REGISTRY', mapping=ip_mapping)

                self.get_broker().set('EVENT_REGISTRY', cached_regristries.get('EVENT_REGISTRY', None))
                self.get_broker().set('PID_REGISTRY', cached_regristries.get('PID_REGISTRY', None))
                self.get_broker().set('IP_REGISTRY', cached_regristries.get('IP_REGISTRY', None))
                return full_event_cache

        def is_globally_unique(self, new_id=None):
            if new_id is None:
                return False, -1
            else:
                unique_ids = self.get_broker().get(self.unique_ids_key)
                if unique_ids is None:
                    unique_ids = []
                    self.get_broker().set(self.unique_ids_key, unique_ids)
                    return True, 0
                else:
                    new_id_val = str(new_id)
                    for i in range(len(unique_ids)):
                        if new_id_val == unique_ids[i]:
                            return False, len(unique_ids)
                    return True, len(unique_ids)

        def get_unique_id(self, new_id=None):
            is_unique, llen = self.is_globally_unique(new_id=new_id)
            if is_unique:
                unique_ids = self.get_broker().get(self.unique_ids_key, convert=True)
                unique_ids.append(str(new_id))
                self.get_broker().set(self.unique_ids_key, unique_ids)
                return True, str(new_id)
            else:
                return False, str(new_id)

        def pub(self, event=None, output=None, recursive=False, pid=None, append_pid=False):
            event_str = event if not append_pid and not pid in event else event + '.' + pid if not pid in event and append_pid else event if pid in event and append_pid else '.'.join(event.split('.')[0:-1])
            event_cache = self.find_cache(event=event_str, process_id=pid, action='PUBLISH')
            if event_cache:
                output_cache = self.get_output(host=event_cache['host'], port=event_cache['port'], cache_type=event_cache['cache_type'])
                output_cache.output(event=event_str, output=output, recursive=recursive)
            else:
                print('no event cache found for event=%s, recursive=%s, pid=%s, append_pid=%s' % (event, str(recursive), str(pid), str(append_pid)))

        def sub(self, event=None, recursive=False, pid=None, append_pid=False):
            event_str = event if not append_pid and not pid in event else event + '.' + pid if not pid in event and append_pid else event if pid in event and append_pid else '.'.join(
                event.split('.')[0:-1])
            event_cache = self.find_cache(event=event_str, process_id=pid, action='SUBSCRIBE')
            if recursive:
                recursive_event = ''
                for evt in event.split('.'):
                    recursive_event += evt + '.'
                    print('Recusrive subscribe to %s ' % recursive_event[0:-1])
                    self.get_publisher(host=event_cache['host'], port=event_cache['port'],
                                           cache_type=event_cache['cache_type']).subscribe(recursive_event[0:-1])
            print('Subscribe to %s' % event_str)
            self.get_publisher(host=event_cache['host'], port=event_cache['port'],
                                   cache_type=event_cache['cache_type']).subscribe(event_str)
            return event_cache, event_str

        def publish_data(self, host=None, port=None, cache_type=None, event=None, output=None, recursive=False):
            self.get_output(host=host, port=port, cache_type=cache_type).output(event=event, output=output,
                                                                                    recursive=recursive)

        def subscribe_to(self, host=None, port=None, cache_type=None, recursive=False, event=None):
            if recursive:
                recursive_event = ''
                for evt in event.split('.'):
                    recursive_event += evt + '.'
                    self.get_publisher(host=host, port=port, cache_type=cache_type).subscribe(recursive_event[0:-1])
            else:
                self.get_publisher(host=host, port=port, cache_type=cache_type).subscribe(event)

        def len(self):
            return len(self.cache_index)

        def keys(self):
            return self.cache_index.keys()

        def host_ports(self):
            d = []
            for k in self.keys():
                _host_, _port_ = k.split('.')
                d.append({'host': _host_, 'port': _port_})
            return d

        def get(self, host=None, port=None, cache_type=None, T='cache'):
            if T == 'cache':
                return CacheManager.instance.get_cache(host=host, port=port, cache_type=cache_type)
            elif T == 'publisher':
                return CacheManager.instance.get_publisher(host=host, port=port, cache_type=cache_type)
            elif T == 'output':
                return CacheManager.instance.get_output(host=host, port=port, cache_type=cache_type)
            else:
                print('Warning :: unable to recognie type %s ' % T)

        def get_host_ports(self):
            return CacheManager.instance.host_ports()

        def publish_data(self, event=None, output=None, recursive=False, pid=None, append_pid=False):
            if output:
                CacheManager.instance.pub(output=output, recursive=recursive, event=event, pid=pid,
                                              append_pid=append_pid)
                return True
            else:
                print('CacheManager Warning (host=%s,port=%s,cache_type=%s) object to publish is not defined....no action.')
                return False

        def subscribe_to(self, event=None, recursive=False, pid=None, append_pid=False):
            if event:
                cache_subscriber, event_str = CacheManager.instance.sub(event=event, recursive=recursive, pid=pid,
                                                                            append_pid=append_pid)
                event_object = dict()
                event_object['append_pid'] = append_pid
                event_object['pid'] = pid
                event_object['event'] = event_str
                event_object['recursive'] = recursive
                event_object['root_event_config'] = cache_subscriber
                event_object['event_config'] = cache_subscriber
                event_object['cache_manager'] = CacheManager.instance
                cache_subscriber = CacheSubscriber(**event_object)
                return True, cache_subscriber
            else:
                print('CacheManager Warning (host=%s,port=%s,cache_type=%s) event to subscribe to is not defined...no action.')
                return False, None

        def get_message(self, host=None, port=None, cache_type=None):
            return CacheManager.instance.get(host=host, port=port, cache_type=cache_type, T='publisher').get_message()

    @staticmethod
    def get_external_ip_addresss():
        try:
            return requests.get('http://ip.42.pl/raw').text
        except:
            print('error getting ip address ')
            traceback.print_exc()
            return None

    @staticmethod
    def broker():
        return CacheManager.instance.get_broker()

    @staticmethod
    def statics():
        json_val = CacheManager.broker().get('statics')
        return  json.loads(json_val) if json_val else dict()

    @staticmethod
    def get_next_statics_listitem():
        return_value = None
        static_buckets = CacheManager.get_state_static_prop(prop='buckets')
        if static_buckets is None:
            static_buckets = {}
        for subkey in static_buckets.keys():
            if return_value is None:
                bucket_list = static_buckets.get(subkey, [])
                if len(bucket_list)>0:
                    return_value = bucket_list.pop()
                    CacheManager.store_state_static_prop(prop='buckets', val=bucket_list, dict_sub=subkey)
                    return return_value
        return return_value

    @staticmethod
    def get_statics_listitems():
        full_list = []
        static_buckets = CacheManager.get_state_static_prop(prop='buckets')
        if static_buckets:
            for subkey in static_buckets.keys():
                full_list += static_buckets.get(subkey, [])
        else:
            static_buckets = {}
            CacheManager.store_state_static_prop(prop='buckets', val=static_buckets)
        return full_list

    @staticmethod
    def add_statics_listitem(prop_id='general', val=None):
        if val: 
            static_buckets = CacheManager.get_state_static_prop(prop='buckets',dict_sub=prop_id)
            if static_buckets is None:
                static_buckets = []   
            if isinstance(val, list):
                static_buckets += val
            else:
                static_buckets.append(val)
            CacheManager.store_state_static_prop(prop='buckets', val=static_buckets , dict_sub=prop_id)
            return True
        return False

    @staticmethod
    def get_statics_listitem(prop_id='general', default_val=None):
        return_value = default_val
        static_buckets = CacheManager.get_state_static_prop(prop='buckets',dict_sub=prop_id)
        if static_buckets is None:
           static_buckets = []   
        if len(static_buckets) > 0:
            return_value = static_buckets.pop()      
            CacheManager.store_state_static_prop(prop='buckets', val=static_buckets , dict_sub=prop_id)
        return return_value

    @staticmethod
    def get_state_static_prop(prop='events', default_value=None, dict_sub=None):
        json_val = CacheManager.statics()
        if json_val:
            if isinstance(json_val.get(prop, default_value), dict) and dict_sub:
                return json_val.get(prop,default_value).get(dict_sub, default_value)
            else:
                return json_val.get(prop, default_value)
        else:
            return default_value

    @staticmethod
    def globals():
        json_val = CacheManager.broker().get(CacheManager.instance.ip)
        return  json.loads(json_val)

    @staticmethod
    def append_val(prop=None, val=None):
        ip_properties_dict = CacheManager.globals()
        if ip_properties_dict is None:
            ip_properties_dict = dict()
        v = ip_properties_dict.get(prop, None)
        if v:
            if isinstance(v, list):
                v.append(val)
        else:
            v=[val]
        if CacheManager.store_state_prop(prop=prop, val=v):
            return v
        else:
            return None

    @staticmethod
    def change_val(prop=None, val=None, adjustment='add'):
        v = CacheManager.get_state_prop(prop=prop)
        if v and isinstance(v, int):
            if adjustment == 'add':
                v += val
            elif adjustment =='subtract':
                v -= val
            elif adjustment =='multiply':
                v *= val
            elif adjustment =='divide':
                v =  v / val
            else:
                return None
            if CacheManager.store_state_prop(prop=prop,val=v):
                return v
            else:
                return None
        else:
            return None

    @staticmethod
    def increment_val( prop=None, val=None ):
        return CacheManager.change_val( prop=prop,val=val,adjustment='add' )

    @staticmethod
    def decrement_val( prop=None, val=None ):
        return CacheManager.change_val( prop=prop, val=val, adjustment='subtract' )

    @staticmethod
    def multiply_val( prop=None, val=None ):
        return CacheManager.change_val( prop=prop, val=val, adjustment='multiply' )

    @staticmethod
    def divide_val( prop=None, val=None ):
        return CacheManager.change_val( prop=prop, val=val, adjustment='divide' )

    @staticmethod
    def get_state_prop( prop='events'):
        try:
            ip_properties_dict = CacheManager.globals()
            if ip_properties_dict is None:
                ip_properties_dict = dict()
                CacheManager.broker().set(CacheManager.instance.ip, ip_properties_dict)
                return None
            v = ip_properties_dict.get(prop, None)
            if v and isinstance(v, str) and v=='True' or v=='true':
                return True
            elif v and isinstance(v, str) and v=='False' or v=='false':
                return False
            return v
        except:
            traceback.print_exc()
            return None

    @staticmethod
    def store_state_static_prop(prop='events', val=None, dict_sub=None):
        def mcon(o):
            if isinstance(o, datetime):
                return o.__str__()
        try:
            ip_properties_dict = CacheManager.statics()
            if ip_properties_dict is None:
                ip_properties_dict = dict()
            if dict_sub is None:
                if val is None:
                    del ip_properties_dict[prop]
                else:
                    ip_properties_dict[prop] = val
            elif not dict_sub is None and isinstance(ip_properties_dict.get(prop, None) , dict) or ip_properties_dict.get(prop, None) is None:
                if ip_properties_dict.get(prop, None) is None:
                    ip_properties_dict[prop] = dict()
                if val is None:
                    del ip_properties_dict[prop][dict_sub]
                else:
                    ip_properties_dict[prop][dict_sub] = val
            elif not dict_sub is None and not isinstance(ip_properties_dict[prop], dict):
                if val is None:
                    del ip_properties_dict[prop]
                else:
                    ip_properties_dict[prop] = val
            else:
                return False
            CacheManager.broker().set( 'statics' , json.dumps(ip_properties_dict, default=mcon) )
            return True
        except:
            traceback.print_exc()
            return False

    @staticmethod
    def store_state_prop(prop='events', val=None, dict_sub=None):
        def mcon(o):
            if isinstance(o, datetime):
                return o.__str__()
        try:
            ip_properties_dict = CacheManager.globals()
            if ip_properties_dict is None:
                ip_properties_dict = dict()
            if dict_sub is None:
                ip_properties_dict[prop] = val
            elif not dict_sub is None and isinstance(ip_properties_dict[prop], dict):
                ip_properties_dict[prop][dict_sub] = val
            elif not dict_sub is None and not isinstance(ip_properties_dict[prop], dict):
                ip_properties_dict[prop] = val
            else:
                return False
            CacheManager.broker().set( CacheManager.instance.ip, json.dumps(ip_properties_dict, default=mcon) )
            return True
        except:
            traceback.print_exc()
            return False

    @staticmethod
    def delete_state_prop(prop='events'):
        try:
            ip_properties_dict = CacheManager.globals()
            if ip_properties_dict is None:
                ip_properties_dict = dict()
            v = ip_properties_dict.get(prop, None)
            if not v is None:
                del ip_properties_dict[v]
                CacheManager.broker().set(CacheManager.instance.ip, ip_properties_dict)
                return True
            return False
        except:
            traceback.print_exc()
            return False


    instance = None

    def __init__(self, **kwargs):
        CacheManager.instance = CacheManager.__CacheManager(**kwargs)
        CacheManager.instance.ip = CacheManager.get_external_ip_addresss()
        self.cache_output_component_func = CacheManager.instance.cache_output_component_func

    @staticmethod
    def get_instance( **kwargs ):
        if CacheManager.instance is None or CacheManager.instance.cache_output_component_func is None:
            CacheManager.instance = CacheManager.__CacheManager(**kwargs)
            kwargs['cache_manager'] = CacheManager.instance
            CacheManager.instance.init_caches(**kwargs)
        return CacheManager.instance






