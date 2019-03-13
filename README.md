# Assaf Python Connector

Turn any python function to a microservice with a config file and minimal boilerplate

This is part of the assaf project  - Generic Model Serving Server

----



Example usage: 

create a basic config file:
```yaml
response:
  json:
    return_key: "data" # key to use in payload
zmq_ipc: "con" 
rabbitmq_host: localhost
rabbitmq_username: guest
rabbitmq_password: guest
rabbitmq_port: 5672
incoming:
   queue_name:  "test" # This is also the application name
   exchange_name: "ds-exchange"
```
```python
import connect as con
con.start({}, lambda profile: profile)

```

Development:

local build: 
```
python3 setup.py sdist bdist_wheel
```
