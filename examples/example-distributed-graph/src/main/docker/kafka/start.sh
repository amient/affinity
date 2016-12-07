#!/bin/bash

/opt/zookeeper/bin/zkServer.sh start /etc/zookeeper/zoo.cfg


LOG_DIR="/var/log/kafka"
JMX_PORT=19092
echo "num.partitions=$KAFKA_NUM_PARTITIONS" >> /etc/kafka/kafka-server.properties

echo "-------------------------------------------------------------"
cat /etc/kafka/kafka-server.properties
echo "-------------------------------------------------------------"
/opt/kafka/bin/kafka-server-start.sh /etc/kafka/kafka-server.properties




