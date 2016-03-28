#!/usr/bin/env python

from distutils.core import setup

setup(name='Kafka-python',
      version='1.0',
      description='Simple kafka python producer/consumer saving to mongo',
      packages=['pymongo', 'kafka-python'],
     )
