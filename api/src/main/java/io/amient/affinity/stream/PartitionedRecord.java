package io.amient.affinity.stream;

public class PartitionedRecord<K,V> {
    public final int partition;
    public final Record<K,V> record;

    public PartitionedRecord(int partition, Record<K,V> record) {
        this.partition = partition;
        this.record = record;
    }
}
