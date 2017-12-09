/*
 * Copyright 2016 Michal Harish, michal.harish@gmail.com
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.amient.affinity.core.storage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The implementing class must provide either no-arg constructor or a constructor that takes two arguments:
 *  Config config
 *  int partition
 */
public abstract class MemStore {

    private AtomicLong size = new AtomicLong(); //can't use LongAdder because we need atomic size snapshot after addition

    public abstract Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator();

    /**
     * @param key ByteBuffer representation of the key
     * @return Some(MV) if key exists
     * None if the key doesn't exist
     */
    public abstract Optional<ByteBuffer> apply(ByteBuffer key);

    /**
     * @return size hint - this may or may not be accurate, depending on the underlying backend's features
     */
    public final long size() {
        return size.longValue();
    }

    /**
     * @param key ByteBuffer representation
     * @param value ByteBuffer which will be associated with the given key
     * @return long size of the memstore (number of keys) after the operation
     */
    public final long put(ByteBuffer key, ByteBuffer value) {
        return putImpl(key, value) ? size.incrementAndGet() : size.get();
    }

    /**
     * @param key ByteBuffer representation
     * @param value ByteBuffer which will be associated with the given key
     * @return true if the key was newly inserted, false if an existing key was updated
     */
    protected abstract boolean putImpl(ByteBuffer key, ByteBuffer value);

    /**
     * @param key ByteBuffer representation whose value will be removed
     * @return long size of the memstore (number of keys) after the operation
     */
    public final long remove(ByteBuffer key) {
        return removeImpl(key) ? size.decrementAndGet() : size.get();
    }

    /**
     * @param key
     * @return true if the key was removed, false if the key didn't exist
     */
    protected abstract boolean removeImpl(ByteBuffer key);


    /**
     * close() will be called whenever the owning storage is closing
     * implementation should clean-up any resources here
     */
    public abstract void close();


    /**
     * Wraps record value with metadata into a storable cell
     * @param value record value
     * @param timestamp record event time
     * @return byte buffer with metadata and record value
     */
    final public ByteBuffer wrap(byte[] value, long timestamp) {
        ByteBuffer memStoreValue = ByteBuffer.allocate(8 + value.length);
        memStoreValue.order(ByteOrder.BIG_ENDIAN);
        memStoreValue.putLong(timestamp);
        memStoreValue.put(value);
        memStoreValue.flip();
        return memStoreValue;
    }

    /**
     * Unwraps stored cell into metadata and value bytes, returning the underlying value only if it hasn't expired
     * with respect to the provided ttl ms parameter and system time
     * @param key record key
     * @param valueAndMetadata wrapped value and event time metadata
     * @param ttlMs time to live of the owner State
     * @return unwrapped byte array of the raw value without metadata if not expired, otherwise none
     */
    final public Optional<byte[]> unwrap(ByteBuffer key, ByteBuffer valueAndMetadata, long ttlMs) {
        if (ttlMs < Long.MAX_VALUE && valueAndMetadata.getLong(0) + ttlMs < System.currentTimeMillis()) {
            //TODO #65 this is the only place where expired records get actually cleaned from the memstore but we need also a regular full compaction process that will get the memstore iterator and call this method
            remove(key);
            return Optional.empty();
        } else {
            int len = valueAndMetadata.limit();
            byte[] result = new byte[len - 8];
            valueAndMetadata.position(8);
            valueAndMetadata.get(result);
            return Optional.of(result);
        }
    }


    /**
     * boostrapping methods: load()
     * @param key record key
     */
    final public void unload(byte[] key) {
        remove(ByteBuffer.wrap(key));
    }

    /**
     * boostrapping methods: unload()
     * @param key record key to be loaded
     * @param value record value to be wrapped
     * @param timestamp event time to be wrapped
     */
    final public void load(byte[] key, byte[] value, long timestamp) {
        ByteBuffer valueBuffer = wrap(value, timestamp);
        put(ByteBuffer.wrap(key), valueBuffer);
    }


}
