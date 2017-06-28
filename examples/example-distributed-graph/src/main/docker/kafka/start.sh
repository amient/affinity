#!/bin/bash

/opt/zookeeper/bin/zkServer.sh start /etc/zookeeper/zoo.cfg

export LOG_DIR="/var/log/kafka"
export JMX_PORT="19092"
export KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote.local.only=false -Djava.rmi.server.hostname=localhost -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.rmi.port=$JMX_PORT "
/opt/kafka/bin/kafka-server-start.sh /etc/kafka/kafka-server.properties




