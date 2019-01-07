# Assaf Python Connector

Turn any python function to a microservice with a config file and minimal boilerplate

This is part of the assaf project  - Generic Model Serving Server

----



### Example usages: 


**Echo Server**:
```python
import connect as con
con.start({}, lambda profile: profile)

```

