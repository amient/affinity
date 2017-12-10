package io.amient.affinity.core.storage;

public class Checkpoint {

    final public long offset;
    final public long size;

    public Checkpoint(long offset, long size) {
        this.offset = offset;
        this.size = size;
    }

    @Override
    public String toString() {
        return "Checkpoint(offset: " + offset +", size: " + size + ")";
    }

    public Checkpoint withSizeDelta(long sizeDelta) {
        return new Checkpoint(offset, size + sizeDelta);
    }
}
