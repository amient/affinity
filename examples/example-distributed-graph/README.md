# Example Graph Data API

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

## Running the ExampleApp

First you'll need Zookeeper and Kafka running locally for which there is a
docker compose file.
    
    cd ./examples/example-distributed-graph
    docker-compose up
 

Then you'll need to create compacted kafka topics, 
e.g. from the Kafka home dir:
 
    ./bin/kafka-topics.sh --zookeeper localhost:2181 --create \
        --topic graph --partitions 4 --replication-factor 1 --config cleanup.policy=compact
    
    ./bin/kafka-topics.sh --zookeeper localhost:2181 --create \
        --topic components --partitions 4 --replication-factor 1 --config cleanup.policy=compact

    ./bin/kafka-topics.sh --zookeeper localhost:2181 --create \
        --topic settings --partitions 1 --replication-factor 1 --config cleanup.policy=compact
        
The graph topic will hold the the main domain data for the app and the
 settings is a global state with a single partition.

Running the `ExampleApp` can be done directly from an IDE or by
running the standalone jar which is created during the gradlew build
 
    java -jar example/build/libs/example-<VERSION>-standalone.jar
    
ExampleApp starts several nodes in a pseudo-distributed mode:
    2 Gateways listen on http ports 8881 and 8882
    4 Regions each serving two physical partitions
    1 Singleton service node for demonstration purpose

Each of the 4 data partitions will 2 online replicas:
  
    -               | Assigned Partitions   |
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
To view a graph vertex (vertex id is a simple Int)

    GET http://127.0.0.1:808x/vertex/<vertex-id> 

To view a graph component (component id is the smallest vertex id in the component) 

    GET http://127.0.0.1:808x/component/<component-id> 

To connect 2 vertices into a component(non-existent vertices will be created):

    POST http://127.0.0.1:808x/connect/<vertex-id>/<vertex-id> 

After connecting two components all vertices that have been already 
connected should be merged into a bigger component. Viewing the 
component by any of the connected vertex ids should show the same group
just in different order.

To disconnect 2 vertices into a component(non-existent vertices will be created):

    POST http://127.0.0.1:808x/disconnect/<vertex-id>/<vertex-id> 

After disconnecting any 2 vertices which were connected previously.
Their respective components will be updated if they were the "bridge"
and the result should be splitting of the component into 2 smaller
components.

To look at the status of the node and which Partition Actors is sees
as partition masters (the addresses may change by node locality but 
physically they should always point to the same actors) 

    GET http://127.0.0.1:<node-port>/status

To look kill a node: 

    POST http://127.0.0.1:<node-port>/kill
    
To look at the partition stats:

    GET http://127.0.0.1:808x/status/<partition-number>

To look kill a region by partition: 

    POST http://127.0.0.1:808x/down/<partition>

