import random
from queue import Queue
import traceback
import json
#Thread Messaging Utils
import requests
class MessagingQueue:
    """
    Messaging between HitParadeBot and WebScraper
    This is how the Thread based classes communicate.
    Each web scraper has an id and that id is the index of all communication with a web scraper.
    direction in denotes communication via input queue
    directiion out denotes communication on output queue
    The main thread listens to the output queue
    The web scraper listens to the input queue

    To send a message to a web scraper with id 44.  send( id=44, direction='in', dict, 'SCRAPE' )
    To send a messages from a web scraper with 44 to the main thread.  Have the WebScraper call send( id=44, direction='out', dict, 'SCRAPE' )

    This class is a singleton.

    """

    class __MessagingQueue:
        """
        Privatized MessagingQueue used in the singleton.
        """

        def __init__(self):
            """
            Constructor
            """
            self.input_queues = dict()
            self.output_queues = dict()
            self.meta_queues = dict()
            self.unique_messages = []
            #maybe change it back not sure
            self.ip = ''#requests.get('http://ip.42.pl/raw').text

        def get_id(self):
            """
            Returns a unique id for a thread to use that has already not been used.
            :return:
            """
            random_int = random.randint(1, 10000)
            while random_int in self.input_queues:
                random_int = random.randint(1, 10000)
            #Set up the queues
            self.input_queues[random_int] = Queue(maxsize=1)
            self.meta_queues[random_int] = Queue(maxsize=1)
            self.output_queues[random_int] = Queue(maxsize=1)
            return self.ip + str(random_int)

        def send(self, id=-1, direction='in', cmd=None, d=None,nowait=False):
            """
            Sends a message
            :param id:  int id of the webscraper
            :param direction: str 'in' for input queue, 'out' for output queue.
            :param cmd: str 'SCRAPE','OPEN','QUIT','LOGIN'
            :param d: dict
            :return: bool True if the message was sent and False if the message was NOT sent.
            """
            try:
                if not id is None:
                    queue_dict = self.input_queues if direction == 'in' else self.output_queues if direction =='out' else self.meta_queues
                    queue = queue_dict.get(id,  queue_dict.get(int(id), None))
                    if not queue is None:
                        if nowait:
                            queue.put_nowait((cmd, d))
                        else:
                            queue.put((cmd, d))
                        return True
                    else:
                        return False
                else:
                    print('id is none warning -')
                    return False
            except:
                traceback.print_exc()
                return False

        def qsize(self, id=-1,direction='in'):
            """
            Gets the metrics of the queue in question if a consumer wants to make a quick get.
            :param direction: str 'in' , 'out', 'meta'
            :return: int,bool,bool   QueueSize, Empty, Full
            """
            try:
                queue_dict = self.input_queues if direction == 'in' else self.output_queues if direction == 'out' else self.meta_queues
                queue = queue_dict.get(id, None)
                if not queue is None:
                    return queue.qsize(), queue.empty(), queue.full()
                else:
                    return -1, False, False
            except:
                traceback.print_exc()

        def send_nowait(self, id=-1, direction='in', cmd=None, d=None):
            return self.send(id=id,direction=direction,cmd=cmd,d=d,nowait=True)

        def quit(self, id=-1):
            """
            Quit command cleans the resources of the id of the specified queue.
            :param id: int id of the webscraper
            :return:
            """
            input_queue = self.input_queues.get(id, None)
            output_queue  = self.output_queues.get(id, None)
            meta_queue = self.meta_queues.get(id, None)
            if not input_queue is None:
                del self.input_queues[id]
            if not output_queue is None:
                del self.output_queues[id]
            if not meta_queue is None:
                del self.meta_queues[id]

    def __init__(self):
        """
        Constructor
        """
        MessagingQueue.instance = MessagingQueue.__MessagingQueue()

    instance = __MessagingQueue()

    @staticmethod
    def q_size(id=-1,direction='in'):
        """
        static method checks the size of a queue.
        :param direction: str 'in','out','meta'
        :return: int,bool,bool queue size, empty, full
        """
        return MessagingQueue.instance.qsize(id=id,direction=direction)

    @staticmethod
    def wait_for_msg(id=-1,direction='in', caller=None, wait=True):
        """
        Statig method:  Wait for the proper queue to have a message and retrieve the message.
        :param direction: str 'in','out','meta'
        :param caller: str 'caller'
        :return: str,str  'key','value'
        """
        try:
            if isinstance(id, int) or isinstance(id, str):
                queue_dict = MessagingQueue.instance.input_queues if direction == 'in' else MessagingQueue.instance.output_queues if direction=='out' else MessagingQueue.instance.meta_queues
                queue = queue_dict.get(id, queue_dict.get(int(id), None))
                if not queue is None:
                    dir = '--->'
                    if direction == 'out':
                        dir = '<---'
                    print('{%s} %s ...waiting from %s' % (id, dir, caller))
                    if wait:
                        return queue.get()
                    else:
                        try:
                            return queue.get_nowait()
                        except:
                            return None, None
        except:
            print('messagingqueue exception -- id is ' % str(id))
            traceback.print_exc()
            return None, None

    @staticmethod
    def get_available_msg_nowait(id=-1,direction='in', caller=None):
        queue_dict = MessagingQueue.instance.input_queues if direction == 'in' else MessagingQueue.instance.output_queues if direction=='out' else MessagingQueue.instance.meta_queues
        queue = queue_dict.get(id, None)
        qsize, e, f = MessagingQueue.instance.qsize(id=id, direction=direction)
        if qsize > 0 and not e and not queue is None:
            dir = '--->'
            if direction == 'out':
                dir = '<---'
            print('GET_NOWAIT::{%s} %s ...waiting from %s' % (id, dir, caller))
            command, obj = queue.get(False)
            qsize, e, f = MessagingQueue.instance.qsize(id=id, direction=direction)
            return command, obj, qsize, e, f
        else:
            return None,None,qsize,e,f

    @staticmethod
    def unique_id(global_id=False, cache_manager=None):
        """
        generates a unique id for use within a scraper or bot.
        :return: int
        """
        if global_id:
            new_id = None
            is_unique = False
            while not is_unique:
                new_id = str(MessagingQueue.instance.get_id())
                is_unique, id_value = cache_manager.get_unique_id(new_id=new_id)
            return new_id
        else:
            return MessagingQueue.instance.get_id()

    @staticmethod
    def quit(id=-1, caller=None):
        """
        quit the resources of the sepcified queue.
        :param id: int id of the webscraper to quit.
        :param caller: str caler of the webscraper
        :return: bool True if quit and False if failed.
        """
        print('quit called from %s for %s ' % (caller, str(id) ))
        return MessagingQueue.instance.quit(id=id)

    @staticmethod
    def send_msg(id=-1,direction='in', cmd=None, d=None, caller=None):
        """
        static method send_message  sends a message and waits if necessary if Queue is full.
        :param id: int id
        :param direction: str 'in','out','meta'
        :param cmd: str command
        :param d: dict data
        :param caller: str caller
        :return:  bool True if sent False otherwise
        """
        dir = '--->'
        if direction == 'out':
            dir = '<---'
        print('{%s} %s [%s] from %s' % (id, dir,cmd, caller))
        return MessagingQueue.send_unique_msg(id=id, direction=direction, cmd=cmd,  d=d,  wait=True)


    @staticmethod
    def send_msg_nowait(id=-1,direction='in', cmd=None, d=None, caller=None):
        """
        sends a message without waiting.
        :param direction: str 'in','out','meta'
        :param cmd: str command
        :param d: dict data object
        :param caller: str 'method caller'
        :return: bool True if sent False otherwise
        """
        dir = '--->'
        if direction == 'out':
            dir = '<---'
        print('{%s} %s [%s] from %s' % (id, dir,cmd, caller))
        return MessagingQueue.send_unique_msg(id=id, direction=direction, cmd=cmd,  d=d,  wait=False)

    @staticmethod
    def send_unique_msg(id=-1, direction='in', cmd=None,  d=None,  wait=True):
        if  d.get('hash', None) is None:
            return MessagingQueue.instance.send(id=id, direction=direction, cmd=cmd, d=d, nowait=(not wait))
        elif not d is None:
            if not d.get('hash', None) in MessagingQueue.instance.unique_messages:
                MessagingQueue.instance.unique_messages.append(d.get('hash', None))
                return MessagingQueue.instance.send(id=id, direction=direction, cmd=cmd, d=d, nowait=(not wait))
            else:
                print('dropping message - wait is true - %s' % json.dumps(d))
                return False
