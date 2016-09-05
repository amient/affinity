# Design Goals 

 - library for building stateful, horizontally scalable REST APIs
 - must be fault-tolerant
 - must be horizontally scalable
 - zero-downtime should be possible
 - can be attached to stream-processing systems 
 - currently a prototype but should become a library 

# Architecture
 

## Cluster Overview

 ![Cluster Architecture](doc/ClusterArchitecture.png)

 - akka for asynchronous communication 
 - akka http as the main interface
 - zookeeper for distributed coordiation

## State Management 

 - kafka (or other) as a fault-tolerant distributed log 
 - pluggable embedded storage with default to RocksDB
...

## Zero-Downtime 

 - multiple nodes can be launched serving the same partition
 - the first one that registers with coordinator will be a `master`
 - the others will become `standby` 
... 



# Examples

## Example Graph API

This example demonstrates a graph data served via API which
maintains connected-components constant time read characterstics.

The underlying topic which back the mem store should have 4 
partitions which can be created from kafka installation home dir:
 
    ./bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic graph \
        --partitions 4 --replication-factor 2 --config cleanup.policy=compact
    
A single node may be started with one region
serving all 4 partitions byt starting `RestApiNode` with
the following arguments:

    ExampleSystem 2551 127.0.0.1 8081 4 0,1,2,3

For pseudo-distributed mode, the `ExampleApp` can be launched
which will run 2 nodes locally each taking 2 of the 4 partitions.


    