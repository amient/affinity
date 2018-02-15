package io.amient.affinity.stream;

public class BinaryRecordAndOffset extends BinaryRecord {
    final public long offset;
    public BinaryRecordAndOffset(byte[] key, byte[] value, long timestamp, long offset) {
        super(key, value, timestamp);
        this.offset = offset;
    }
}
