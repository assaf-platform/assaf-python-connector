
import json
import zmq
import asyncio
import logging
from time import time
from zmq.asyncio import Context

from pika import BasicProperties as PikaBasicProperties
from pika.adapters.asyncio_connection import IOLoopAdapter

from connect.RabbitMqConnector import MessageQueue
from connect.RabbitMqConnector import RabbitMqListener
from connect.RabbitMqConnector import PikaClient

from connect.config import config as cfg

logging.getLogger("pika").setLevel(logging.WARNING)


def handle_request(pubsuber, service, zmq_ipc_address):
    context = Context.instance()
    socket = context.socket(zmq.PULL)
    zmq_add = "ipc://" + zmq_ipc_address
    socket.connect(zmq_add)
    def f():
        print("checking messages on zmq....")
        # print("f got argument:", frame)
        logging.info("entering loop")
        future_q = socket.recv_multipart()
        print("got future from receive:", future_q)
        future_q.add_done_callback(send_to_subs(pubsuber, service))

        # return future_q

    return f


def send_to_subs(pubsuber, service):
    properties = {"app_id":pubsuber.routing_key,
                 "content_type":'application/json'}

    def do_ds_query(incoming_message):
        print(incoming_message)
        query = "".join([s.decode("UTF-8") for s in incoming_message.result()])
        print("got profile: ", query)
        try:
            data_request = json.loads(query)
            query_body = data_request["body"]
            options = take_first_from_lists(data_request["options"])
            request_id = data_request["request-id"]
            options.update({"profile": query_body})
            response = predict(request_id, query_body, service, options)
            response_json = json.dumps(response)
            res = bytes(response_json, "UTF-8")
            properties
        except Exception:
            logging.exception("got error while doing ds query")
            res = bytes("oops", "UTF_8")
        print('publishing to', pubsuber.exchange_name, pubsuber.routing_key)
        pubsuber.channel.basic_publish(pubsuber.exchange_name, str(request_id),
                                       res,
                                       PikaBasicProperties(**properties))

    return do_ds_query


def predict(request_id, req_body, service, options):
    r = service(**options)
    return resp(req_body, r, request_id)

def take_first_from_lists(dict):
    return {k: v[0] for k, v in dict.items()}


def resp(query, res, request_id):
    return {"version": 1,
            "timestamp": time(),
            "profile-name": query,
            "request-id": request_id,
            cfg["response"]["json"]["return_key"]: res
            }


def normalize_names(*name):
    return ".".join([n.replace("-", "") for n in name])


def start(conf, service):
    cfg.update(conf.items())
    incoming_queue_name = cfg["incoming"]["queue_name"]
    incoming_exchange_name = cfg["incoming"]["exchange_name"]
    outgoing_routing_key = normalize_names(incoming_exchange_name, incoming_queue_name)
    zmq_ipc_address = cfg["zmq_ipc"]
    message_listener_config = {
        "exchange_name": incoming_exchange_name,
        "exchange_type": 'direct',
        "queue_name": incoming_queue_name,
        "routing_key": incoming_queue_name,
        "exchange": {
            "isDurable": True,
            "isAutoDelete": False
        },
        "queue": {
            "isExclusive": False,
            "isDurable": True,
            "isAutoDelete": False
        }
    }

    pubsub_config = {"exchange_name": 'pubsub',
                     "exchange_type": 'topic',
                     "queue_name": 'response_pubsub',
                     "routing_key": outgoing_routing_key,
                     "exchange": {
                         "isDurable": False,
                         "isAutoDelete": True
                     },
                     "queue": {
                         "isExclusive": False,
                         "isDurable": False,
                         "isAutoDelete": True
                     }
                     }
    pubsub = RabbitMqListener(pubsub_config)

    messages = MessageQueue(RabbitMqListener(message_listener_config), ipc_address=zmq_ipc_address)
    task = handle_request(pubsub, service, zmq_ipc_address)
    # Monkey patch task
    messages.task = task
    curr_loop = asyncio.get_event_loop()
    loop = IOLoopAdapter(curr_loop)
    rabbit_url = cfg["rabbitmq_host"]  # localhost
    rabbit_port = cfg["rabbitmq_port"]  # 32769
    rabbit_vhost = "/"
    rabbit_username = cfg["rabbitmq_username"]  # "guest"
    rabbut_password = cfg["rabbitmq_password"]  # "guest"
    client = PikaClient(rabbit_url,
                        rabbit_port,
                        rabbit_vhost,
                        rabbit_username,
                        rabbut_password,
                        loop=loop,
                        listeners=[pubsub, messages])

    # Monkey patch client
    messages.client = client
    try:
        client.connect()
        client.connection.ioloop.add_callback_threadsafe(task)
        client.connection.ioloop.start()
    except KeyboardInterrupt:
        logging.info("W: interrupt received, stopping…")
        exit(0)
    except Exception:
        logging.exception("something went wrong")
        client.connection.close()


if __name__ == '__main__':
    start({}, lambda profile: profile)
