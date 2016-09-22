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
 - On becoming a Master, the Partition Actor stops consuming (tailng) 
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

## Example Graph Data API

This example demonstrates a graph data served via Affinity REST API 
which maintains connected-components constant time read characteristics.

In the connected components any new connection added is propagated to all
other connected vertexes, which could be done either with 
Akka ! Tell - this would result in very fast response time on updates 
but with potential data loss. In the example, instead, Akka ? Ask 
is used for the propagation which slows down the writes
but results in consistent state when there is a recoverable failure.

NOTE: An irrecoverable failure, like a machine crash while the 
propagation is in progress would still result in an inconsistent state.
As it stands now even the best effort to guarantee consistency fails 
if the updates are done in the fast Akka layer but this is simply the 
nature of the example. In terms of architecture there is no write-ahead 
log for the computation and if there is a need for exactly-once
semantics or other strong guarantees, the problem can be delegated to
 a dedicated stream-processor by simply connecting it to the same
  topics as are used for affinity storage change logs! 

### Running the ExampleApp

First you'll need Zookeeper and Kafka running locally. Then
you'll need to create 2 compacted kafka topics, 
e.g. from the Kafka home dir:
 
    ./bin/kafka-topics.sh --zookeeper localhost:2181 --create \
        --topic graph --partitions 4 --replication-factor 1 --config cleanup.policy=compact
    
    ./bin/kafka-topics.sh --zookeeper localhost:2181 --create \ 
        --topic settings --partitions 1 --replication-factor 1 --config cleanup.policy=compact
        
The graph topic will hold the the main domain data for the app and the
 settings is a broadcast topic with a single partition.

Running the `ExampleApp` can be done directly from an IDE or by
running the standalone jar which is created during the gradlew build
 
    java -jar example/build/libs/example-<VERSION>-standalone.jar
    
ExampleApp starts several nodes in a pseudo-distributed mode:
    2 Gateways listen on http ports 8081 and 8082
    4 Regions each serving two physical partitions
    1 Singleton service node for demonstration purpose

Each of the 4 data partitions will 2 online replicas:
  
                    | Assigned Partitions   |
    ----------------+-----+-----+-----+-----+
    Data Partition: |  0  |  1  |  2  |  3  |
    ----------------+-----+-----+-----+-----+               
    Region 1        |  x  |  x  |  -  |  -  |
    Region 2        |  -  |  x  |  x  |  -  |
    Region 3        |  -  |  -  |  x  |  x  |
    Region 4        |  x  |  -  |  -  |  x  |

Cooridnation process on startup chooses one of the replicas as
 master and the other standby. Using the `kill region` endpoint
 below it can be demonstrated how the zero-down time works by
 standby immediately taking over the killed master.
 
The example data set is a Graph of Vertices which are connected
to other Vertices. A group of inter-connected vertices is a Component.
To view a graph component (vertex id is a simple Int)

    GET http://127.0.0.1:808x/vertex/<vertex-id> 

To connect 2 vertices into a component(non-existent vertices will be created):

    POST http://127.0.0.1:808x/connect/<vertex-id>/<vertex-id> 

After connecting two components all vertices that have been already 
connected should be merged into a bigger component. Viewing the 
component by any of the connected vertex ids should show the same group
just in different order.

To look at the status of the node and which Partition Actors is sees
as partition masters (the addresses may change by node locality but 
physically they should always point to the same actors) 

    GET http://127.0.0.1:<node-port>/

To look kill a node: 

    POST http://127.0.0.1:<node-port>/kill
    
To look at the partition stats:

    GET http://127.0.0.1:808x/<partition-number>

To look kill a region by partition: 

    POST http://127.0.0.1:808x/<partition>/down

