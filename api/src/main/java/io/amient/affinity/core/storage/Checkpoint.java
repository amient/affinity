package io.amient.affinity.core.storage;

public class Checkpoint {

    final private long offset;
    final private long size;

    public Checkpoint(long offset, long size) {
        this.offset = offset;
        this.size = size;
    }
}
