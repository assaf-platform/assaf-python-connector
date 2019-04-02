
import json
import zmq
import asyncio
import logging
from time import time
from zmq.asyncio import Context

from pika import BasicProperties as PikaBasicProperties

from connect.RabbitMqConnector import MessageQueue
from connect.RabbitMqConnector import RabbitMqListener
from connect.RabbitMqConnector import PikaClient

from jaeger_client import Config
from opentracing.propagation import Format

from connect.config import config as cfg

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
        "app_id":pubsuber.routing_key,
        "content_type" : 'application/json',
        "headers": {}
    }

    def do_ds_query(incoming_message):

        message = "".join([s.decode("UTF-8") for s in incoming_message.result()])
        logging.info(message)
        query_and_header = message.split('😏👀🍆')
        query = query_and_header[0]
        header = query_and_header[1]
        pspan = tracer.extract(Format.TEXT_MAP, {"uber-trace-id": header})
        work_span = tracer.start_span('messagePassedToModel', child_of=pspan)
        # logging.info(query)
        logging.info("got profile: %s", query)
        try:
            data_request = json.loads(query)
            query_body = data_request["body"]
            options = take_first_from_lists(data_request["options"])
            request_id = data_request["request-id"]
            options.update({"profile": query_body})
            response = predict(request_id, query_body, service, options)
            response_json = json.dumps(response)
            res = bytes(response_json, "UTF-8")
        except Exception as e:
            logging.exception("got error while doing ds query")
            res = bytes(str(e), "UTF_8")
        logging.info('publishing to: %s %s', pubsuber.exchange_name, pubsuber.routing_key)
        publish_span = tracer.start_span('messagePublishedBackToFrontEnd', child_of=work_span)
        properties["headers"]["uber-trace-id"] = str(publish_span).split(" ")[0]
        pubsuber.channel.basic_publish(pubsuber.exchange_name, str(request_id),
                                       res,
                                       PikaBasicProperties(**properties))
        publish_span.finish()
        work_span.finish()




    return do_ds_query


def predict(request_id, req_body, service, options):
    r = service(**options)
    return resp(req_body, r, request_id)

def take_first_from_lists(dict):
    new_dict = {}
    for k,v in dict.items():
        if len(v) == 1:
            new_dict[k] = v[0]
        else:
            new_dict[k] = v

    return new_dict


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

    config = Config(
        config={  # usually read from some yaml config
            'sampler': {
                'type': 'const',
                'param': 1,
            },
            'logging': True,
        },
        service_name=cfg["incoming"]["queue_name"],
        validate=True,
    )
    # this call also sets opentracing.tracer
    tracer = config.initialize_tracer()
    pubsub = RabbitMqListener(pubsub_config)

    messages = MessageQueue(RabbitMqListener(message_listener_config), tracer=tracer, ipc_address=zmq_ipc_address)
    task = handle_request(pubsub, service, zmq_ipc_address, tracer)
    # Monkey patch task
    messages.task = task
    curr_loop = asyncio.get_event_loop()
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
                        loop=curr_loop,
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
