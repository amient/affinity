package io.amient.affinity.stream;

public class BinaryRecord extends Record<byte[], byte[]> {
    public BinaryRecord(byte[] key, byte[] value, long timestamp) {
        super(key, value, timestamp);
    }
}
