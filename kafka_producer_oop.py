import threading, sys, json, time
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer

class Producer:
	'Simple kafka producer'

	def __init__(self, servers, topic):
		self.client = KafkaClient(servers)
		self.producer = SimpleProducer(self.client)
		self.topic = topic

	def current_time(self):
		return int(round(time.time() * 1000))

	def produce(self, mesnum):
		msg = json.dumps({'time': self.current_time(), 'data' : 'Hello - %s' % mesnum})
		self.producer.send_messages(self.topic, msg)

	def stop(self):
		if self.producer != None:
			self.producer.stop()
		if self.client != None:
			self.client.close()

if __name__ == '__main__':
	servers = sys.argv[1]
	topic = sys.argv[2]
	mesnum = int(sys.argv[3])
	interval = float(sys.argv[4])

	producer = Producer(servers, topic, mesnum)

	try:
		while mesnum > 0:
			producer.produce(mesnum)
			time.sleep(interval)
			mesnum -= 1
	except KeyboardInterrupt:
		print "Interrupted!"
	finally:
		producer.stop()
