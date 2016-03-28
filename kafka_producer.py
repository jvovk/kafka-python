#!/usr/bin/python

import sys
from kafka import KafkaProducer
import threading

servers = sys.argv[1]
topic = sys.argv[2]

def printit():
  threading.Timer(1.0, printit).start()
  producer.send(topic, 'Hello, world!') 

producer = KafkaProducer(bootstrap_servers=servers)
printit()
