class CacheSubscriber:

    def __init__(self, **kwargs):
        self.event = kwargs.get('event', None)
        self.recursive = kwargs.get('recursive', False)
        self.pid = kwargs.get('pid', None)
        self.append_pid = kwargs.get('append_pid', False)
        self.root_config = kwargs.get('root_config', kwargs.get('root_event_config', None))
        self.event_config = kwargs.get('event_config', None)
        self.cache_manager = kwargs.get('cache_manager', None)

    def next_message(self):
        config_object = self.root_config
        if self.append_pid:
            config_object = self.event_config
        if self.root_config is None:
            config_object = self.event_config
        return self.cache_manager.get_message(host=config_object['host'], port=config_object['port'], cache_type=config_object['cache_type'])


    def next_messages(self):
        messages = {}
        message = {
            'data' : b'123'
        }
        while message and not isinstance(message['data'], int) and message['data'].decode('utf-8').isdigit():
            config_object = self.root_config
            if self.append_pid:
                config_object = self.event_config
            if self.root_config is None:
                config_object = self.event_config
            message = self.cache_manager.get_message(host=config_object['host'], port=config_object['port'], cache_type=config_object['cache_type'])
            if message and not isinstance(message['data'], int) and message['data'].decode('utf-8').isdigit():
                k = str(message['data'])
                if messages.get(k, None) is None:
                    messages[k] = message
        return [messages[k] for k in messages.keys()]
