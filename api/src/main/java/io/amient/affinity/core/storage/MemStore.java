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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * The implementing class must provide either no-arg constructor or a constructor that takes two arguments:
 *  Config config
 *  int partition
 */
public abstract class MemStore {

    public abstract Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator();

    /**
     * @param key ByteBuffer representation of the key
     * @return Some(MV) if key exists
     * None if the key doesn't exist
     */
    public abstract Optional<ByteBuffer> apply(ByteBuffer key);

    /**
     * @param key ByteBuffer representation
     * @param value ByteBuffer which will be associated with the given key
     * @return optional value held at the key position before the update
     */
    public abstract Optional<ByteBuffer> update(ByteBuffer key, ByteBuffer value);

    /**
     * @param key ByteBuffer representation whose value will be removed
     * @return optional value held at the key position before the update, None if the key doesn't exist
     */
    public abstract Optional<ByteBuffer> remove(ByteBuffer key);

    /**
     * close() will be called whenever the owning storage is closing
     * implementation should clean-up any resources here
     */
    public abstract void close();


    /**
     * Wraps record value with metadata into a storable cell
     * @param value
     * @param timestamp
     * @return
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
     * @param valueAndMetadata
     * @param ttlMs
     * @return
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
     * boostrapping methods: load(), unload()
     */

    final public void unload(byte[] key) {
        remove(ByteBuffer.wrap(key));
    }

    final public void load(byte[] key, byte[] value, long timestamp) {
        ByteBuffer valueBuffer = wrap(value, timestamp);
        update(ByteBuffer.wrap(key), valueBuffer);
    }


}
