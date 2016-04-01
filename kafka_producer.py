import threading, sys, json, time
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer
from datetime import datetime

if __name__ == "__main__":
	servers = sys.argv[1]
	topic = sys.argv[2]
	mesnum = int(sys.argv[3])
	interval = float(sys.argv[4])

	client = KafkaClient(servers)
	producer = SimpleProducer(client)

	current_time = lambda: int(round(time.time() * 1000))

	try:
		while mesnum > 0:
			msg = json.dumps({'time': current_time(), 'data' : 'Hello - %s' % mesnum})
			producer.send_messages(topic, msg)
			time.sleep(interval)
			mesnum -= 1
	except KeyboardInterrupt:
		print "Interrupted!"
	finally:
		producer.stop()
		client.close()
