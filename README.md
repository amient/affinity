# Design Goals 

 - library for building stateful, scalable REST APIs
 - can be attached to stream-processing systems 
 - fault-tolerance build on top of a distributed-log
 - horizontally scalable
 - zero-downtime possible
 
# Architecture
 

## Cluster Overview

 ![Cluster Architecture](doc/affinity.png)

 - akka for asynchronous communication 
 - akka http as the main interface
 - zookeeper for distributed coordiation
 - kafka as a fault-tolerant change log

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

The codebase is split into core which is the general purpose library 
for this architecture and an example application which uses the core 
library. 

The following core features are already in place:

 - HTTP Interface is completely async done with Akka Http. 
 - HTTP Handlers participate in handling the incoming HTTP Requests
    by chaining the receive: Receive method of the Gateway Actor
 - Handlers translate requests into Akka Messages - they can either ? Ask
    and get response which they turn into HTTP Response or just ! Tell
    and respond with No Content or Accepted, etc.
 - Akka Cluster that comes with Akka is not used, instead a custom
    cluster management is implemented 
 - Each Gateway Actor System has a Cluster Actor in its hierarchy which 
    implements standard Akka Router interface with custom routing logic.
    This routing logic is meant to mimic whatever partitioning strategy
    is used in the underlying kafka storage.
 - Cluster is therefore a dynamic Akka Router which maintains a copy of the 
    active Partition Actors, kept up to date by a pluggable Coordinator 
    (ZkCoordinator by default)
 - Each Handler has access to the Cluster Actor by extending the Gateway
    as mentioned above. Any task that needs to be handled by a partition
    is given to the Cluster Actor. This may be a simple forward or 
    it can be an orchestrated sequence of Asks and Tells.
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
 

## Testing the code

    ./gradlew test    
    
## Building the project
        
    ./gradlew build

## JavaScript (affinity.js)

There is affinity_node.js file which contains the source for avro
web socket implementation. It is based on a node avsc.js library
which requires browserify tool to generate browser version:

    npm install -g browserify

When working on this script the browser script affinity.js can
be generated then by:

    browserify example/src/main/resources/affinity_node.js -o example/src/main/resources/affinity.js
    
When doing a lot of work on the javascript watchify can be used
  to automatically generate the new affinity.js when the affinity_node.js
  is modified:

    npm install -g watchify
    watchify example/src/main/resources/affinity_node.js -v -o example/src/main/resources/affinity.js -d
    
//TODO use gradle-node-plugin to generate affinity.js during build 