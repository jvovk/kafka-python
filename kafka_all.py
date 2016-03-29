#!/usr/bin/python

import threading, logging, time, sys

from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer
from pymongo import MongoClient
from datetime import datetime


servers = sys.argv[1]
topic = sys.argv[2]
mongohost = sys.argv[3]

class Producer(threading.Thread):
	daemon = True

	def run(self):
		client = KafkaClient(servers)
		producer = SimpleProducer(client)

		while True:
			producer.send_messages(topic, "Hello!")
			time.sleep(1)


class Consumer(threading.Thread):
	daemon = True

	def run(self):
		client = MongoClient([mongohost])
		db = client.messages
		client = KafkaClient(servers)
		consumer = SimpleConsumer(client, "test-group", topic)

		for msg in consumer:
			print msg
			db.message.insert_one(
				{ 
					"date": '{:%m/%d/%Y %H/%M/%S"}'.format(datetime.now()),
					"message": msg.message.value
				}
			)

def main():
	threads = [
		Producer(),
		Consumer()
	]

	for t in threads:
		t.start()

	time.sleep(10)

if __name__ == "__main__":
	logging.basicConfig(
		format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
		level=logging.DEBUG
	)
	main()


