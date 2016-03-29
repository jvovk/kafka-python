#!/usr/bin/python

import time, threading, sys
from pymongo import MongoClient
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer


servers = sys.argv[1]
topic = sys.argv[2]
mongohost = sys.argv[3]

class Producer(threading.Thread):
	daemon = True

	def run(self):
		producer = KafkaProducer(bootstrap_servers=servers)
		while True:
			producer.send(topic, "Hello, world!")
			time.sleep(1)


class Consumer(threading.Thread):
	daemon = True

	def run(self):
		client = MongoClient([mongohost])
		db = client.messages
		consumer = KafkaConsumer(bootstrap_servers=servers)
		consumer.subscribe([topic])
		for msg in consumer:
			db.message.insert_one(
				{ 
					"date": '{:%m/%d/%Y %H/%M/%S"}'.format(datetime.now()),
						"message": msg.value
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
	main()




