import threading, sys, json
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row

def check_and_write(x):
	try:
		x.toDF().write.format("org.apache.spark.sql.cassandra").options(table="test1", keyspace = "mykeyspace").save(mode ="append") 
	except ValueError:
		print "No rdd found!"

if __name__ == '__main__':
	servers = sys.argv[1]
	topic = sys.argv[2]
	casshost = sys.argv[3]
	interval = int(sys.argv[4])
	zookeeper = sys.argv[5]

	conf = SparkConf().setAppName("KafkaSpark").set("spark.cassandra.connection.host", casshost)
	sc   = SparkContext(conf=conf)
	sqlContext = SQLContext(sparkContext=sc)
	ssc = StreamingContext(sc, batchDuration=interval)

	messages = KafkaUtils.createStream(ssc, zookeeper, "spark-streaming-consumer", {topic: 1})
	lines = messages.map(lambda x: x[1])

	rows = lines.map(lambda x: { 
		"data": json.loads(x)['data'],
		"time": json.loads(x)['time']
		})

	rows.foreachRDD(lambda x: {
		check_and_write(x)
		})

	ssc.start()
	ssc.awaitTermination()

