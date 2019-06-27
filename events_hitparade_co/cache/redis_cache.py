import json
import redis

class RedisCache:
    def __init__(self, **kwargs):
        self.host = kwargs.get('host', 'hitparade001.et5b4b.0001.use1.cache.amazonaws.com' )
        self.port = kwargs.get('port', 6379)
        self.cache_type = kwargs.get('cache_type', None)
        self.publishers = dict()
        self.subscribers = dict()
        self.redis_instance  = redis.StrictRedis(host=self.host, port=self.port)  # Connect to local Redis instance
        self.publisher = self.redis_instance.pubsub()  # See https://github.com/andymccurdy/redis-py/#publish--subscribe
        self.pub_sub_mechanism = self.redis_instance.pubsub()
        self.clear = kwargs.get('clear', False)
        if self.clear:
            self.redis_instance.flushdb()

    EVENT_SEPARATOR = '.'

    @staticmethod
    def event_separator():
        return RedisCache.EVENT_SEPARATOR

    def set(self, key=None, val=None):
        if isinstance(val, str):
            self.redis_instance.set(key,val)
            return True
        elif isinstance(val, dict) or  isinstance(val, list):
            self.redis_instance.set(key, json.dumps(val))
            return True
        else:
            return False

    def get(self, key=None, convert=False):
        if key:
            if convert:
                return json.loads(self.redis_instance.get(key))
            else:
                return self.redis_instance.get(key)
        else:
            return None

    def hmset(self, key=None, dict_value=None):
        if key and dict_value:
            self.redis_instance.hset(key.lower().strip(), dict_value)
            return True
        return False

    def hmgetall(self, key=None):
        if key:
            return self.redis_instance.hgetall(key.lower().strip())
        return None

    def pubsub(self):
        return self.pub_sub_mechanism

    def publish_data(self, event_string=None, payload=None, recursive=True):
        if not event_string is None and not payload is None:
            if recursive:
                new_event_string = ''
                event_segments = event_string.split(RedisCache.event_separator())
                for segment in event_segments:
                    if new_event_string == '':
                        new_event_string = segment
                    else:
                        new_event_string += RedisCache.event_separator() + segment 
                    print('publishing payload to %s ' % new_event_string)
                    self.redis_instance.publish(new_event_string, payload)

            else:
                print('publishing payload to %s ' % event_string)
                self.redis_instance.publish(event_string, payload)
