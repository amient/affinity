#FROM openjdk:alpine
FROM anapsix/alpine-java

ARG num_partitions
ENV kafka_num_partitions=$num_partitions

RUN mkdir -p /var/zookeeper/data
RUN mkdir -p /var/log/zookeeper
RUN mkdir -p /etc/zookeeper
COPY zoo.cfg /etc/zookeeper/

RUN mkdir -p /volume/kafka/data
RUN mkdir -p /var/log/kafka
RUN mkdir -p /etc/kafka
COPY kafka-server.properties /etc/kafka/
COPY kafka-log4j.properties /etc/kafka/

RUN mkdir -p /opt
COPY build.sh /
RUN chmod u+x /build.sh
RUN /bin/sh /build.sh

################################


ENV KAFKA_LOG4J_OPTS "-Dlog4j.configuration=file:/etc/kafka/kafka-log4j.properties"
ENV ZOO_LOG4J_PROP='ERROR,ROLLINGFILE'
ENV ZOO_LOG_DIR="/var/log/zookeeper"

#VOLUME ["/volume/kafka"]

WORKDIR /

CMD /opt/kafka/bin/kafka-server-start.sh /etc/kafka/kafka-server.properties &> /var/log/kafka/kafka.out &

COPY start.sh /


EXPOSE 2181
EXPOSE 9092 19092

ENTRYPOINT /start.sh










