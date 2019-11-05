- decouple online partitions from storage paritions (requires AvroRDD instead of LogRDD on the spark side)
- try leveraging the consumer rebalance algorithm - listening to assignment changes and then overriding the starting offset from the offset stored in the local file system..

By default the consumer will start fetching messages for its assigned
// partitions at this point, but your application may enable rebalance
// events to get an insight into what the assigned partitions where
// as well as set the initial offsets. To do this you need to pass
// `"go.application.rebalance.enable": true` to the `NewConsumer()` call
// mentioned above. You will (eventually) see a `kafka.AssignedPartitions` event
// with the assigned partition set. You can optionally modify the initial
// offsets (they'll default to stored offsets and if there are no previously stored
// offsets it will fall back to `"auto.offset.reset"`
// which defaults to the `latest` message) and then call `.Assign(partitions)`
// to start consuming.
And probably using https://github.com/dgraph-io/badger as the local storage..