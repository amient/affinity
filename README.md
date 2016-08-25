# Design Goals 

- library for building stateful, horizontally scalable REST APIs
- must be fault-tolerant
- must be horizontally scalable
- zero-downtime should be possible
- can be attached to stream-processing systems 
- currently a prototype but should become a library 

# Architecture
 

## Cluster Architecture
 ![DXPSpark](doc/DXPSpark.png)
 
## State Architecture

...


- akka for asynchronous communication 
- akka http as the main interface
- zookeeper for distributed coordiation
- kafka as a fault-tolerant distributed log 
- pluggable embedded storage with default to RocksDB
- TODO draw distribution diagram
