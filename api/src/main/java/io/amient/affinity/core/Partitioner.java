package io.amient.affinity.core;

public interface Partitioner {
    int partition(byte[] serializedKey, int numPartitions);
}
