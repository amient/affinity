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

