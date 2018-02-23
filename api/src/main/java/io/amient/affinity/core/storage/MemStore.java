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

import io.amient.affinity.core.config.CfgCls;
import io.amient.affinity.core.config.CfgPath;
import io.amient.affinity.core.config.CfgStruct;
import io.amient.affinity.core.util.ByteUtils;
import io.amient.affinity.core.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * The implementing class must provide a constructor that takes two arguments:
 * StateConf conf
 */
public abstract class MemStore implements Closeable {

    public static class MemStoreConf extends CfgStruct<MemStoreConf> {
        public CfgCls<MemStore> Class = cls("class", MemStore.class, true);
        public CfgPath DataDir = filepath("data.dir", false);

        //TODO public CfgInt MemReadTimeoutMs = integer("memstore.read.timeout.ms", 1000);
        @Override
        protected Set<String> specializations() {
            return new HashSet(Arrays.asList("mapdb", "rocksdb"));
        }
    }

    private final static Logger log = LoggerFactory.getLogger(MemStore.class);

    final private boolean checkpointsEnable;
    final protected int ttlSecs;
    final protected Path dataDir;

    public MemStore(StateConf conf) throws IOException {
        checkpointsEnable = isPersistent();
        ttlSecs = conf.TtlSeconds.apply();
        if (!checkpointsEnable) {
            dataDir = null;
        } else {
            dataDir = conf.MemStore.DataDir.apply();
            if (!Files.exists(dataDir)) Files.createDirectories(dataDir);
        }
    }

    protected abstract boolean isPersistent();

    public abstract CloseableIterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator();

    /**
     * @param key ByteBuffer representation of the key
     * @return Some(MV) if key exists
     * None if the key doesn't exist
     */
    public abstract Optional<ByteBuffer> apply(ByteBuffer key);

    /**
     * This may or may not be accurate, depending on the underlying backend's features
     *
     * @return number of keys in the store
     */
    public abstract long numKeys();

    /**
     * Store value
     *
     * @param key      ByteBuffer representation
     * @param value    ByteBuffer which will be associated with the given key
     */
    public abstract void put(ByteBuffer key, ByteBuffer value);

    /**
     * remove key
     *
     * @param key      ByteBuffer representation whose value will be removed
     */
    public abstract void remove(ByteBuffer key);


    /**
     * Wraps record value with metadata into a storable cell
     *
     * @param value     record value
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
     *
     * @param key              record key
     * @param valueAndMetadata wrapped value and event time metadata
     * @param ttlMs            time to live of the owner State
     * @return unwrapped byte record if not expired, otherwise none
     */
    final public Optional<Record<byte[], byte[]>> unwrap(ByteBuffer key, ByteBuffer valueAndMetadata, long ttlMs) {
        long ts = valueAndMetadata.getLong(0);
        if (ttlMs > 0 && valueAndMetadata.getLong(0) + ttlMs < System.currentTimeMillis()) {
            //this is the magic that expires key-value pairs based on their create timestamp
            //State.iterator also invokes unwrap for each entry therefore simply iterating cleans up expired entries
            remove(key);
            return Optional.empty();
        } else {
            int len = valueAndMetadata.limit();
            byte[] value = new byte[len - 8];
            valueAndMetadata.position(8);
            valueAndMetadata.get(value);
            return Optional.of(new Record<>(ByteUtils.bufToArray(key), value, ts));
        }
    }

}
