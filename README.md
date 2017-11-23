# Design Goals 

 - library for building stateful, scalable Data APIs on top of streaming foundation
 - can be attached to stream-processing pipelines based around kafka and participate
   either as a producer/source or consumer/materializer of state
 - fault-tolerance build on top of a distributed-log
 - horizontally scalable and fully asynchronous
 - zero-downtime possible
 

# Architecture
 

## Cluster Overview

 ![Cluster Architecture](doc/affinity.png)

 - akka for asynchronous communication 
 - akka http as the main interface with websocket layer
 - zookeeper for distributed coordination (other coordinators pluggable)
 - RocksDb, MemDb, ConcurrentMap and other MemStore implementations
 - kafka as a fault-tolerant change log storage

## State Management

All data is stored in the physical storage in the form
of change log, e.g. Kafka compacted topic. Each physical partition
can have multiple active API Partitions, one of which is 
always a master and the remainder are standby(s). 

In the most consistent setup, master takes all reads and writes 
and records each write in the storage change log while reads
come directly from the in-memory data set.
After storage has accepted the write, master updates its own 
in-memory state - this way the level of data loss acceptance can
be controlled by simply configuring the appropriate ack level
in the underlying storage.

Standby(s) tail the changelog continuously and keep their 
in-memory state up-to-date. In in the event of master failure, 
one of the standby(s) is picked to be the new master. This way
zero-downtime is possible for both failures as well as upgrades.

In cases where eventual read consistency is sufficient, standby(s) 
can also be used as read replicas (this is currently not implemented
but the design is expecting this to come in future).


# Development 

The codebase is split into several modules:

 - `api` is the internal programming api and utiliities for writing memstore and storage plugins (Java)
 - `avro` scala case class <--> avro automatic conversion with schema registry wrappers (conf,zk,local,embedded) (Scala)
 - `core` is the main scala library with js-avro extension (Scala)
 - `examples/..` contain example applications (Scala)
 - `kafka/kafka_10/avro-formatter-kafka` kafka formatter for console consumer for the `avro` module (Scala)
 - `kafka/kafka_10/avro-serde-kafka` kafka produer serializer and consumer deserializer for the `avro` module (Scala) 
 - `kafka/kafka_10/storage-kafka` module with kafka storage and confluent schema provider (Scala)
 - `kafka/kafka_10/test-util-kafka` provides EmbeddedZooKeeper, EmbeddedKafka and EmbeddedCfRegistry for testing
 - `mapdb` module with MapDb implementation of the MemStore (Java)
 - `rocksdb` module with RocksDb implementation of the MemStore (Java)
 - `spark/spark_2.0` exposes underlying kafka storage as KafkaRDD with all the serde magic
 - `ws-client` custom web socket with avro support written (Java)
 
 
The following core features are already in place:

 - HTTP Interface is completely async done with Akka Http. 
 - HTTP Handlers participate in handling the incoming HTTP Requests
    by chaining the receive: Receive method of the Gateway Actor
 - Handlers translate requests into Akka Messages - they can either ? Ask
    and get response which they turn into HTTP Response or just ! Tell
    and respond with No Content or Accepted, etc.
 - WebSockets can be attached to Key-Value entities which then receive 
    automatically all changes to that entity as well as any other 
    user-defined events 
 - Akka Cluster that comes with Akka is not used, instead a custom
    cluster management is implemented using a third-party coordinator
 - Each Gateway Actor System maintains a list of Service Actors 
    in its hierarchy which implement the the routing logic. This routing 
    logic mimics the partitioning strategy used in the underlying storage.
 - Each Service is therefore a dynamic Akka Router which maintains a copy 
    of the active Partition Actors, kept up to date by a pluggable 
    Coordinator (ZooKeeper and Embedded embedded implementations provided)
 - Each Handler has access to all Service Actors by extending the Gateway
    as mentioned above. Any task that needs to be handled by a partitioned
    logic is passed to the Service Actor. This may be a simple forward or 
    it can be an orchestrated sequence of Asks and Tells.
 - Gateways are hence the orchestration layer and Services are individual
    microservices or keyspaces whose logic is completely constrained to
    the partition scope.
 - Cluster Actor routes all request to Partition Actors which implement
    the logic over the data partition and respond to the sender which
    will ultimately be the calling Handler but sometimes the caller 
    may be other Services in the cluster. The partition doesn't have
    any knowledge of the larger cluster it is part of.
 - If there are multiple Partition Actors for the same physical partition
    Coordinator uses distributed logic to choose one of them as master
    and the others become standby.    
 - On becoming a Master, the Partition Actor stops consuming (tailing) 
    the the underlying topic, because the master receives all the writes, 
    its in-memory state is consistent and it only publishes to the kafka 
    for future bootstrap and keeping other standby(s) for the partitions 
    up to date.
 - On becoming a Standby, the Partition Actor resumes consuming the 
     underlying topic and stops receiving until it again becomes a master.
 - Standby is not a read replica at the moment but it could be an option
 - custom Akka ack implementation which is strongly typed
 - Lightweight Transactions can be wrapped around orchestrated logic
    which use reversible Instructions to rollback failed operations    
 - Support for different Avro schema providers which are implementations of 
    a generalised concept similar to Confluent Schema Registry (for which
    an implementation is provided) - this allows to use embedded schema 
    provider for development and testing while in production can be 
    deployed with full distributed schema registry. These avro registries
     are implemented with standard Akka Serialisation and are at the same
     time compatible with Confluent Schema Registry which means the same
     serialization is used for Akka transport, MemStores and Storage.
 

## Testing the code

    ./gradlew test    
    
## Building the project
        
    ./gradlew build

## JavaScript (affinity.js)

There is affinity_node.js file which contains the source for avro
web socket implementation. It is based on a node avsc.js library:

    npm install avsc


To generate final affinity.js for borwsers:

    npm install -g browserify

When working on this script the browser script affinity.js can
be generated then by:

    browserify core/src/main/resources/affinity_node.js -o core/src/main/resources/affinity.js
    
When doing a lot of work on the javascript watchify can be used
  to automatically generate the new affinity.js when the affinity_node.js
  is modified:

    npm install -g watchify
    watchify core/src/main/resources/affinity_node.js -v -o core/src/main/resources/affinity.js -d
 
## Avro Schemas

..

### Avro Kafka Serializer and Deserializer

...