package io.amient.affinity.core.storage;

public class LogEntry<POS extends Comparable<POS>>  extends Record<byte[], byte[]> {
    final public POS position;
    public LogEntry(POS position, byte[] key, byte[] value, long timestamp) {
        super(key, value, timestamp);
        this.position = position;
    }

}
