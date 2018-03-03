## io.amient.affinity.kafka.AvroMessageFormatter


### Packaging

In order to use this formatter you need to build the following jar and place it in the classpath of kafka distribution:

    ./gradlew :kafka:avro-formatter-kafka:assemble

    cp ./kafka/avro-formatter-kafka/build/lib/avro-formatter-kafka-*-all.jar $KAFKA_HOME/libs


### Usage

    kafka-console-consumer.sh \
      --bootstrap-server <...> 
      --topic <...> \
      --formatter io.amient.affinity.kafka.AvroMessageFormatter \
     [--property schema.registry.url=<CONFLUENT-SCHEMA-REGISTRY-URL> \]
     [--property schema.registry.zookeeper.connect=<ZOOKEEPER-SCHEMA-REGISTRY-CONNECT> \]
     [--property schema.registry.zookeeper.root=<ZOOKEEPER-SCHEMA-REGISTRY-ROOT> \]
     [--property pretty ]
     [--property print.partition]
     [--property print.offset]
     [--property print.key] 
     [--property print.timestamp]
     [--property no.value]

