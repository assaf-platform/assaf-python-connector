# Assaf Python Connector

Turn any python function to a microservice with a config file and minimal boilerplate

This is part of the assaf project  - Generic Model Serving Server

----



Example usage: 

```python
import connect as con
con.start({}, lambda profile: profile)

```

Development:

local build: 
```
python3 setup.py sdist bdist_wheel
```
