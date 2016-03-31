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

	try:
		while mesnum > 0:
			ts = time.time()
			t = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S');
			msg = json.dumps({'time': t, 'data' : 'Hello - %s' % mesnum})
			producer.send_messages(topic, msg)
			time.sleep(interval)
			mesnum -= 1
	except KeyboardInterrupt:
		print "Interrupted!"
	finally:
		producer.stop()
		client.close()
