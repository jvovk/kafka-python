#!/usr/bin/python

import sys
from kafka import KafkaConsumer
from pymongo import MongoClient
from datetime import datetime

servers = sys.argv[1]
topic = sys.argv[2]
mongohost = sys.argv[3]

client = MongoClient([mongohost])
db = client.messages
consumer = KafkaConsumer(topic, bootstrap_servers=servers)
for msg in consumer:
	print msg
	db.message.insert_one(
				{ 
				  "date": '{:%m/%d/%Y %H/%M/%S"}'.format(datetime.now()),
				  "message": msg.value
				}
	 )