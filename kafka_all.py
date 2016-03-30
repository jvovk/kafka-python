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
mesnum = int(sys.argv[4])
i = mesnum

client = KafkaClient(servers)
producer = SimpleProducer(client)
consumer = SimpleConsumer(client, "test-group", topic)
cl = MongoClient([mongohost])
db = cl.messages
rescount = 0

class Producer(threading.Thread):
	daemon = True

	def run(self):
		global i
		global rescount
		while i > 0:
			response = producer.send_messages(topic, "Hello!")
			if response != None:
				rescount += 1;
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


def close():
	if client != None:
		client.close()

	if producer != None:
		producer.stop()

	if consumer != None:
		consumer.stop()

	if cl != None:	
		cl.close()


def main():
	p = Producer()
	c = Consumer()

	p.start()
	c.start()

	p.join()
	c.stop()

	dbmescount = db.message.count()

	if rescount == mesnum == dbmescount:
		code = 0
	elif rescount == 0:
		code = 1
	elif rescount < mesnum and rescount == dbmescount:
		code = 2
	else:
		code = 3

	db.message.delete_many({})

	close()

	sys.exit(code)


if __name__ == "__main__":
	logging.basicConfig(
		format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
		level=logging.DEBUG
	)
	main()

