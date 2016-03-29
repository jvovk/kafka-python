#!/bin/bash

cd /Users/YV/Downloads/kafka_2.10-0.9.0.0
sudo bin/zookeeper-server-start.sh -daemon config/zookeeper.properties 
PID1=$!
sudo bin/kafka-server-start.sh -daemon config/server.properties 
PID2=$!
sudo mongod --fork --syslog
PID3=$!

if [ -n "PID2" ]; then 
	python /Users/YV/Documents/python/kafka-python/kafka_all.py localhost:9092 test localhost:27017

strcount=$(mongo --host="localhost:27017" messages -eval 'db.message.count({})')
strcount=$(echo $strcount | awk '{print $8}')
echo $strcount
if [ $strcount -ge 10 ]; then 
	exit_code=0
	echo All messages were saved. SUCCESS.
else 
	exit_code=1
	echo Not all messages were saved. ERROR.
fi

mongo --host="localhost:27017" messages -eval 'db.message.remove({})' &> /dev/null

pr=$(ps -e | awk '/kafka/{print $1}')
sudo kill -KILL $pr &> /dev/null

pr=$(ps -e | awk '/mongod/{print $1}')
sudo kill -KILL $pr &> /dev/null

exit $exit_code
else
	exit 1
fi