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

client = KafkaClient(servers)
producer = SimpleProducer(client, async=True)
consumer = SimpleConsumer(client, "test-group", topic)

class Producer(threading.Thread):
	daemon = True

	def run(self):
		i = 10
		while i > 0:
			producer.send_messages(topic, "Hello!")
			time.sleep(1)
			i -= 1
		self.stop()


	def __init__(self):
		super(Producer, self).__init__()
		self._stop = threading.Event()

	def stop(self):
		self._stop.set()


class Consumer(threading.Thread):
	daemon = True

	def run(self):
		cl = MongoClient([mongohost])
		db = cl.messages

		for msg in consumer:
			db.message.insert_one(
				{ 
					"date": '{:%m/%d/%Y %H/%M/%S"}'.format(datetime.now()),
					"message": msg.message.value
				}
			)
	

	def __init__(self):
		super(Consumer, self).__init__()
		self._stop = threading.Event()

	def stop(self):
		self._stop.set()


def main():
	p = Producer()
	c = Consumer()

	p.start()
	c.start()

	p.join()
	c.stop()

	if client is not None:
		client.close()

if __name__ == "__main__":
	logging.basicConfig(
		format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
		level=logging.DEBUG
	)
	main()

