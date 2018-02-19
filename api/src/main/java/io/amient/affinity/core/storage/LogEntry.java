package io.amient.affinity.core.storage;

public class LogEntry<P extends Comparable<P>>  extends Record<byte[], byte[]> {
    final public P position;
    public LogEntry(P position, byte[] key, byte[] value, long timestamp) {
        super(key, value, timestamp);
        this.position = position;
    }

}
