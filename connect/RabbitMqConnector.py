import logging
import pika
from zmq.asyncio import Context
from zmq import PUSH
class PikaClient(object):
    def __init__(self,
                 host,
                 port,
                 virtual_host,
                 username,
                 password,
                 listeners,
                 loop):
        self.host = host
        self.port = port
        self.virtual_host = virtual_host
        self.username = username
        self.password = password
        self.loop = loop
        # Listeners are a list of objects that know how
        # to process messages
        self.listeners = listeners

        self.connected = False
        self.connecting = False
        self.connection = None
        self.channel = None

    def connect(self):
        if self.connecting:
            return

        self.connecting = True

        credentials = pika.PlainCredentials(self.username, self.password)
        param = pika.ConnectionParameters(host=self.host,
                                          port=self.port,
                                          virtual_host=self.virtual_host,
                                          credentials=credentials)
        self.connection = pika.SelectConnection(param,
                                                on_open_callback=self.on_connected,
                                                custom_ioloop=self.loop)
        self.connection.add_on_close_callback(self.on_closed)

    def on_connected(self, connection):
        self.connected = True
        self.connection = connection
        self.connection.channel(self.on_channel_open)

    def on_channel_open(self, channel):
        for listener in self.listeners:
            listener.start(channel)

    def on_closed(self, connection):
        connection.close()


class ListenerConfig(object):
    def __init__(self, config: dict):
        """

        :type config: dict
        """
        self.exchange_name = config["exchange_name"]
        self.exchange_type = config["exchange_type"]
        self.queue_name = config["queue_name"]
        self.queue_exclusive = config["queue"]["isExclusive"]
        self.queue_durable = config["queue"]["isDurable"]
        self.queue_auto_delete = config["queue"]["isAutoDelete"]
        self.routing_key = config["routing_key"]
        self.exchange_durable = config["exchange"]["isDurable"]
        self.exchange_auto_delete = config["exchange"]["isAutoDelete"]


class RabbitMqListener(ListenerConfig):

    def __init__(self, listenerConfigDict):
        # self.exchange_name = exchange_name
        # self.exchange_type = exchange_type
        # self.queue_name = queue_name
        # self.routing_key = routing_key
        self.channel = None
        ListenerConfig.__init__(self, listenerConfigDict)
        self.config = listenerConfigDict

    def start(self, channel):
        self.channel = channel
        self.channel.exchange_declare(exchange=self.exchange_name,
                                      exchange_type=self.exchange_type,
                                      auto_delete=self.exchange_auto_delete,
                                      durable=self.exchange_durable,
                                      callback=self.on_exchange_declared)

    def on_exchange_declared(self, frame):
        logging.info("channel opened %s", frame)
        self.channel.queue_declare(exclusive=self.queue_exclusive,
                                   durable=self.queue_durable,
                                   auto_delete=self.queue_auto_delete,
                                   callback=self.on_queue_declared,
                                   queue=self.queue_name)

    def on_queue_declared(self, frame):
        self.queue_name = frame.method.queue
        logging.info("queue: %s", frame.method.queue)
        self.channel.queue_bind(exchange=self.exchange_name,
                                queue=self.queue_name,
                                routing_key=self.routing_key,
                                callback=self.on_queue_bind)

    def on_queue_bind(self, frame):
        pass


class MessageQueue(RabbitMqListener):
    def __init__(self,
                 listener: RabbitMqListener,
                 ipc_address):
        RabbitMqListener.__init__(self, listener.config)
        context = Context.instance()
        self.socket = context.socket(PUSH)
        self.socket.bind("ipc://" + ipc_address)

    def on_queue_bind(self, frame):
        self.channel.basic_consume(self.on_message, self.routing_key)

    def on_message(self, channel, method_frame, header_frame, body):
        self.socket.send(body)
        self.client.connection.ioloop.add_callback_threadsafe(self.task)
        print("msg sent")
        self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)
