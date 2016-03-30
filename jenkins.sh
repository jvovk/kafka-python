#!/bin/bash

cd /Users/YV/Downloads/kafka_2.10-0.9.0.0
sudo bin/zookeeper-server-start.sh config/zookeeper.properties > /Users/YV/Documents/log-file.log 2>&1 &
sudo bin/kafka-server-start.sh config/server.properties > /Users/YV/Documents/log-file-2.log 2>&1 &
sudo mongod --fork --syslog > /dev/null 2>&1  
sleep 5

mesnum=10

python /Users/YV/Documents/python/kafka-python/kafka_all.py localhost:9092 test localhost:27017 $mesnum > /Users/YV/Documents/log-file-4.log 2>&1 

code=$?

if [[ $code -eq 0 ]]; then
	echo "$mesnum messages were delievered and saved. SUCCESS"
elif [[ $code -eq 1 ]]; then 
	echo "0 messages were delievered. FAIL"
elif [[ $code -eq 2 ]]; then
	echo "Less than $mesnum messages, but all of them were saved. "
else 
	echo "Count of messages delievered are not equal count of saved. "
fi 

sudo bin/kafka-server-stop.sh
sleep 10
sudo bin/zookeeper-server-stop.sh

pid=$(ps -e | awk '/mongod --fork/{print $1}')
sudo kill -KILL $pid > /dev/null 2>&1

exit $code