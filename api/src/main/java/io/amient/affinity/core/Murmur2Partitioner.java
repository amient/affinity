package io.amient.affinity.core;

import io.amient.affinity.core.util.ByteUtils;

public class Murmur2Partitioner implements Partitioner {

    public int partition(byte[] serializedKey, int numPartitions) {
        int result = (ByteUtils.murmur2(serializedKey)  & 0x7fffffff) % numPartitions;
        return result;
    }

}
