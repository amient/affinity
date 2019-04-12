## io.amient.affinity.kafka.ConsumerGroupReporter


### Packaging

In order to use this formatter you need to build the following jar and place it in the classpath of kafka distribution:

    ./gradlew :kafka:metrics-reporter:assemble

    cp ./kafka/metrics-reporter/build/lib/metrics-reporter-kafka-*-all.jar $KAFKA_HOME/libs

### Download

    https://repo1.maven.org/maven2/io/amient/affinity/metrics-reporter-kafka_<KAFKA-MINOR-VERSION>/0.10.1/metrics-reporter-kafka_<KAFKA-MINOR-VERSION>-0.10.1-all.jar

### Usage

Put the packaged/downloaded -all jar into the kafka broker libs/ and add the following lines to the kafka server.properties

    kafka.metrics.reporters=io.amient.kafka.metrics.ConsumerGroupReporter
    kafka.metrics.polling.interval=<NUM-SECONDS>    
    kafka.metrics.bootstrap.servers=<LOCAL-LISTENER>
    #if sasl is enabled for local listeners
    #kafka.metrics.security...
    #kafka.metrics.sasl.mechanism=
    #kafka.metrics.sasl.jaas.config=

This will produce additional kafka metrics objects under `kafka.consumer` jmx namespace 
    
