FROM phusion/baseimage
RUN apt-get update -y

RUN echo "deb http://download.opensuse.org/repositories/network:/messaging:/zeromq:/release-stable/Debian_9.0/ ./" >> /etc/apt/sources.list
RUN curl https://download.opensuse.org/repositories/network:/messaging:/zeromq:/release-stable/Debian_9.0/Release.key | apt-key add
RUN apt-get install -y libzmq3-dev

COPY dist/synonyms_generator-0.1.0-py2.py3-none-any.whl /dist/synonyms_generator-0.1.0-py2.py3-none-any.whl

COPY dist/assaf_connect-0.0.1-py3-none-any.whl /dist/assaf_connect-0.0.1-py3-none-any.whl
RUN apt-get install -y python3-pip
RUN pip3 install /dist/synonyms_generator-0.1.0-py2.py3-none-any.whl
RUN pip3 install /dist/assaf_connect-0.0.1-py3-none-any.whl

CMD ["python3"]

