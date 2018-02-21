package io.amient.affinity.core.storage;

import java.io.Serializable;

public class Record<K, V> implements Serializable {
    public final K key;
    public final V value;
    public final long timestamp;

    public Record(K key, V value, long timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

}