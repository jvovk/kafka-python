#!/bin/bash

cd /Users/YV/Downloads/kafka_2.10-0.9.0.0
sudo bin/zookeeper-server-start.sh config/zookeeper.properties > /Users/YV/Documents/log-file.log 2>&1 &
PID1=$!
sudo bin/kafka-server-start.sh config/server.properties > /Users/YV/Documents/log-file-2.log 2>&1 &
PID2=$!
sudo mongod --fork --syslog > /dev/null 2>&1  
sleep 7

python /Users/YV/Documents/python/kafka-python/kafka_all.py localhost:9092 test localhost:27017 > /Users/YV/Documents/log-file-4.log 2>&1 

strcount=$(mongo --host="localhost:27017" messages -eval 'db.message.count({})')
strcount=$(echo $strcount | awk '{print $8}')

if [[ $strcount -ge 10 ]]; then 
	code=0
	echo All messages were saved. SUCCESS.
else 
	code=1
	echo Not all messages were saved. ERROR.
fi

mongo --host="localhost:27017" messages -eval 'db.message.remove({})' > /dev/null 2>&1  

sudo kill $PID2 > /dev/null 2>&1  
sudo kill $PID1 > /dev/null 2>&1  
pid=$(ps -e | awk '/server.properties/{print $1}')
sudo kill $pid > /dev/null 2>&1 
pid=$(ps -e | awk '/zookeeper.properties/{print $1}')
sudo kill $pid > /dev/null 2>&1 

pid=$(ps -e | awk '/mongod --fork/{print $1}')
sudo kill $pid > /dev/null 2>&1

exit $code