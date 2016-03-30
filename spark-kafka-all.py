import threading, logging, time, sys
import pyspark_cassandra
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer
from datetime import datetime
from pyspark import SparkConf, SparkContext

servers = sys.argv[1]
topic = sys.argv[2]
casshost =  sys.argv[3]
mesnum = int(sys.argv[4])
i = mesnum
list = []

conf = SparkConf().setAppName("KafkaSpark").setMaster("local[*]").set("spark.cassandra.connection.host", casshost)
sc   = SparkContext(conf=conf)

client = KafkaClient(servers)
producer = SimpleProducer(client)
consumer = SimpleConsumer(client, "test-group", topic)
rescount = 0

class Producer(threading.Thread):
	daemon = True

	def run(self):
		global i
		global rescount
		while i > 0:
			response = producer.send_messages(topic, "Hello-%s!" % i)
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
			list.append(msg.message.value)
	

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

	if sc != None:	
		sc.stop()


def main():
	p = Producer()
	c = Consumer()

	p.start()
	c.start()

	p.join()
	c.stop()

	time = datetime.now()
	row = sc.parallelize(list).zipWithUniqueId().map(lambda x: {
		"id": x[1],
		"data": x[0],
		"time": time
		})
	row.saveToCassandra("mykeyspace", "test1")

	close()


if __name__ == "__main__":
	main()