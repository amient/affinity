package io.amient.affinity.core.storage;

public class LogEntry<C>  extends Record<byte[], byte[]> {
    final public C position;
    public LogEntry(C position, byte[] key, byte[] value, long timestamp) {
        super(key, value, timestamp);
        this.position = position;
    }

}
