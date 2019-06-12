import json
import zmq
import asyncio
import logging
from zmq.asyncio import Context

from pika import BasicProperties as PikaBasicProperties
from pika.adapters.asyncio_connection import IOLoopAdapter

from connect.RabbitMqConnector import MessageQueue
from connect.RabbitMqConnector import RabbitMqListener
from connect.RabbitMqConnector import PikaClient

from opentracing.propagation import Format

from connect.config import config as cfg
from connect.response import init_jaeger_tracer, resp
from connect.utils import normalize_names, take_first_from_lists

logging.getLogger("pika").setLevel(logging.WARNING)


def handle_request(pubsuber, service, zmq_ipc_address, tracer):
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
        future_q.add_done_callback(send_to_subs(pubsuber, service, tracer))

        # return future_q

    return f


def send_to_subs(pubsuber, service, tracer):
    properties = {
        "app_id": pubsuber.routing_key,
        "content_type": 'application/json',
        "headers": {}
    }

    def do_ds_query(incoming_message):

        message = "".join([s.decode("UTF-8") for s in incoming_message.result()])
        logging.info(message)
        ### The assaf python connector protocol is
        ### MESSAGE😏👀🍆UBER_TRACE_ID
        query_and_header = message.split('😏👀🍆')
        query = query_and_header[0]
        trace_id = query_and_header[1]
        pspan = tracer.extract(Format.TEXT_MAP, {"uber-trace-id": trace_id})
        work_span = tracer.start_span('messagePassedToModel', child_of=pspan)
        # logging.info(query)
        try:
            # 1. load data
            data_request = json.loads(query)
            logging.info("got profile: %s", query)
            query_body = data_request["body"]
            options = take_first_from_lists(data_request["options"])
            request_id = data_request["request-id"]
            options.update({"profile": query_body})
            work_span.log_kv({"query": query_body, "options": options})
            # 2. call provided function using arguments
            response = predict(request_id, query_body, service, options)
            response_json = json.dumps(response)
            # We need bytes for rabbitmq
            res = bytes(response_json, "UTF-8")
            properties["headers"]["http-status"] = 200
        except KeyError as e:
            logging.exception("Key not found")
            properties["headers"]["http-status"] = 404
            res = bytes(str(e), "UTF_8")
        except Exception as e:
            logging.exception("got error while doing ds query")
            properties["headers"]["http-status"] = 500
            res = bytes(str(e), "UTF_8")
        work_span.finish()
        logging.info('publishing to: %s %s', pubsuber.exchange_name, pubsuber.routing_key)
        publish_span = tracer.start_span('messagePublishedFromService', child_of=pspan)
        properties["headers"]["uber-trace-id"] = str(publish_span).split(" ")[0]
        publish_span.log_kv({"response": res.decode("UTF-8")})
        pubsuber.channel.basic_publish(pubsuber.exchange_name, str(request_id),
                                       res,
                                       PikaBasicProperties(**properties))
        publish_span.finish()

    return do_ds_query


def predict(request_id, req_body, service, options):
    r = service(**options)
    return resp(req_body, r, request_id)


def init_listeners(conf, service):
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

    tracer = init_jaeger_tracer(incoming_queue_name)
    pubsub = RabbitMqListener(pubsub_config)
    task = handle_request(pubsub, service, zmq_ipc_address, tracer)
    messages = MessageQueue(RabbitMqListener(message_listener_config), tracer=tracer, ipc_address=zmq_ipc_address)
    return  pubsub, messages, task

def start(conf, service):
    pubsub, messages, task = init_listeners(conf, service)

    # Monkey patch task
    messages.task = task
    curr_loop = asyncio.get_event_loop()
    loop = IOLoopAdapter(curr_loop)
    rabbit_url = cfg["rabbitmq_host"]
    rabbit_port = cfg["rabbitmq_port"]
    rabbit_vhost = "/"
    rabbit_username = cfg["rabbitmq_username"]
    rabbut_password = cfg["rabbitmq_password"]
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
    except KeyboardInterrupt as ki:
        logging.info("W: interrupt received, stopping…")
    except Exception as e:
        logging.exception("something went wrong")
    finally:
        import os
        loop.stop()
        loop.close()
        curr_loop.shutdown_asyncgens()
        os._exit(1)


if __name__ == '__main__':
    def my(profile):
        return 3


    start({}, my)
