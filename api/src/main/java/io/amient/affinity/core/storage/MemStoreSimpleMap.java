package io.amient.affinity.core.storage;

import io.amient.affinity.core.util.CloseableIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class MemStoreSimpleMap extends MemStore {

    final private ConcurrentHashMap<ByteBuffer, ByteBuffer> internal = new ConcurrentHashMap<>();

    public MemStoreSimpleMap(StateConf conf) throws IOException {
        super(conf);
    }

    @Override
    protected boolean isPersistent() {
        return false;
    }

    @Override
    public CloseableIterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator() {
        return CloseableIterator.apply(internal.entrySet().iterator());
    }

    @Override
    public Optional<ByteBuffer> apply(ByteBuffer key) {
        return Optional.ofNullable(internal.get(key));
    }

    @Override
    public long numKeys() {
        return internal.size();
    }

    @Override
    public void put(ByteBuffer key, ByteBuffer value) {
        internal.put(key, value);
    }

    @Override
    public void remove(ByteBuffer key) {
        internal.remove(key);
    }

    @Override
    public void close() throws IOException {
        internal.clear();
    }
}
