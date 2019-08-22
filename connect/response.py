from time import time

from jaeger_client import Config

from connect.config import config as cfg


def init_jaeger_tracer(service_name='your-app-name'):
    c = {  # usually read from some yaml config
        'sampler': {
            'type': 'const',
            'param': 1,
        },
        'logging': True,
    }

    config = Config(config=c, service_name=service_name, validate=True)
    tracer = config.initialize_tracer()
    return tracer


def resp(query, res, request_id):
    return {"version": 1,
            "timestamp": time(),
            "profile-name": query,
            "request-id": request_id,
            cfg["response"]["json"]["return_key"]: res
            }