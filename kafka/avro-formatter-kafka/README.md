## io.amient.affinity.kafka.AvroMessageFormatter


### Packaging

In order to use this formatter you need to build the following jar and place it in the classpath of kafka distribution:

    ./gradlew :kafka:avro-formatter-kafka:assemble

    cp ./kafka/avro-formatter-kafka/build/lib/avro-formatter-kafka-*-all.jar $KAFKA_HOME/libs


### Download

#### Scala 2.12
    
    https://repo1.maven.org/maven2/io/amient/affinity/avro-formatter-kafka_<KAFKA-MINOR-VERSION>-scala_2.12/0.10.2/avro-formatter-kafka_<KAFKA-MINOR-VERSION>-scala_2.12-0.10.2-all.jar

#### Scala 2.11

    https://repo1.maven.org/maven2/io/amient/affinity/avro-formatter-kafka_<KAFKA-MINOR-VERSION>-scala_2.11/0.10.2/avro-formatter-kafka_<KAFKA-MINOR-VERSION>-scala_2.12-0.10.2-all.jar

### Usage

    kafka-console-consumer.sh \
      --bootstrap-server <...> 
      --topic <...> \
      --formatter io.amient.affinity.kafka.AvroMessageFormatter \
     [--property schema.registry.url=<SCHEMA-REGISTRY-URL> \]
     [--property schema.registry.zookeeper.connect=<ZOOKEEPER-SCHEMA-REGISTRY-CONNECT> \]
     [--property schema.registry.zookeeper.root=<ZOOKEEPER-SCHEMA-REGISTRY-ROOT> \]
     [--property pretty ]
     [--property print.partition]
     [--property print.type]
     [--property print.offset]
     [--property print.key] 
     [--property print.timestamp]
     [--property no.value]