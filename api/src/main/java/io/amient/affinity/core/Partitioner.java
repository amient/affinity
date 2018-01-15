package io.amient.affinity.core;

public interface Partitioner {
    int partition(Object key, byte[] serializedKey, int numPartitions);
}
