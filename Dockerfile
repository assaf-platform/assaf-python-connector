FROM phusion/baseimage
RUN apt-get update -y

RUN echo "deb http://download.opensuse.org/repositories/network:/messaging:/zeromq:/release-stable/Debian_9.0/ ./" >> /etc/apt/sources.list
RUN curl https://download.opensuse.org/repositories/network:/messaging:/zeromq:/release-stable/Debian_9.0/Release.key | apt-key add
RUN apt-get install -y libzmq3-dev

COPY setup.py  libs/connector

ADD connect/ /libs/connector/connect
RUN apt-get install -y python3-pip
RUN pip3 install app/connect

CMD ["python3"]

