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

package io.amient.affinity.core.storage.rocksdb;

import io.amient.affinity.core.config.CfgStruct;
import io.amient.affinity.core.util.CloseableIterator;
import io.amient.affinity.core.storage.MemStore;
import io.amient.affinity.core.storage.StateConf;
import io.amient.affinity.core.util.ByteUtils;
import org.rocksdb.*;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;


public class MemStoreRocksDb extends MemStore {

    private final static org.slf4j.Logger log = LoggerFactory.getLogger(MemStoreRocksDb.class);

    public static class MemStoreRocksDbConf extends CfgStruct<MemStoreRocksDbConf> {

        public MemStoreRocksDbConf() {
            super(MemStoreConf.class);
        }
        //TODO on 64-bit systems memory mapped files can be enabled
    }

    private static Map<Path, Long> refs = new HashMap<>();
    private static Map<Path, RocksDB> instances = new HashMap<>();

    synchronized private static final RocksDB createOrGetDbInstanceRef(Path pathToData, Options rocksOptions, int ttlSecs) {
        RocksDB.loadLibrary();
        if (refs.containsKey(pathToData) && refs.get(pathToData) > 0) {
            refs.put(pathToData, refs.get(pathToData) + 1);
            return instances.get(pathToData);
        } else {

            try {
                log.info("Opening RocksDb with TTL=" + ttlSecs);
                RocksDB instance = ttlSecs > 0 ? TtlDB.open(rocksOptions, pathToData.toString(), ttlSecs, false)
                        : TtlDB.open(rocksOptions, pathToData.toString());
                //RocksDB instance = RocksDB.open(rocksOptions, pathToData.toString());
                instances.put(pathToData, instance);
                refs.put(pathToData, 1L);
                return instance;
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        }
    }

    synchronized private static final void releaseDbInstance(Path pathToData) {
        if (refs.get(pathToData) > 1) {
            refs.put(pathToData, refs.get(pathToData) - 1);
        } else {
            refs.remove(pathToData);
            instances.get(pathToData).close();
        }
    }

    private final Path pathToData;
    private final RocksDB internal;

    @Override
    protected boolean isPersistent() {
        return true;
    }

    public MemStoreRocksDb(StateConf conf) throws IOException {
        super(conf);
        pathToData = dataDir.resolve(this.getClass().getSimpleName());
        log.info("Opening RocksDb MemStore: " + pathToData);
        Files.createDirectories(pathToData);
        MemStoreRocksDbConf rocksDbConf = new MemStoreRocksDbConf().apply(conf.MemStore);
        Options rocksOptions = new Options().setCreateIfMissing(true);
        if (conf.MemStore.KeyPrefixSize.isDefined()) {
            rocksOptions.useCappedPrefixExtractor(conf.MemStore.KeyPrefixSize.apply());
        }
        internal = createOrGetDbInstanceRef(pathToData, rocksOptions, ttlSecs);
    }

    @Override
    public CloseableIterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator(ByteBuffer prefix) {
        byte[] prefixBytes = prefix == null ? null : ByteUtils.bufToArray(prefix);
        return new CloseableIterator<Map.Entry<ByteBuffer, ByteBuffer>>() {
            private RocksIterator rocksIterator = null;
            private boolean checked = false;

            @Override
            public boolean hasNext() {
                checked = true;
                if (rocksIterator == null) {
                    rocksIterator = internal.newIterator();
                    if (prefixBytes == null) {
                        rocksIterator.seekToFirst();
                    } else {
                        rocksIterator.seek(prefixBytes);
                    }

                } else {
                    rocksIterator.next();
                }
                if (prefixBytes == null) {
                    return rocksIterator.isValid();
                } else {
                    return rocksIterator.isValid() && ByteUtils.startsWith(rocksIterator.key(), prefixBytes);
                }
            }

            @Override
            public Map.Entry<ByteBuffer, ByteBuffer> next() {
                if (!checked) {
                    if (!hasNext()) throw new NoSuchElementException("End of iterator");
                }
                checked = false;
                return new AbstractMap.SimpleEntry<>(
                        ByteBuffer.wrap(rocksIterator.key()), ByteBuffer.wrap(rocksIterator.value())
                );
            }

            @Override
            public void close() throws IOException {
                if (rocksIterator != null) rocksIterator.close();
            }
        };
    }

    @Override
    public Optional<ByteBuffer> apply(ByteBuffer key) {
        return get(ByteUtils.bufToArray(key));
    }

    @Override
    synchronized public void put(ByteBuffer key, ByteBuffer value) {
        byte[] keyBytes = ByteUtils.bufToArray(key);
        try {
            internal.put(keyBytes, ByteUtils.bufToArray(value));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long numKeys() {
        try {
            return internal.getLongProperty("rocksdb.estimate-num-keys");
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    synchronized public void remove(ByteBuffer key) {
        byte[] keyBytes = ByteUtils.bufToArray(key);
        try {
            internal.remove(keyBytes);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        releaseDbInstance(pathToData);
    }

    private Optional<ByteBuffer> get(byte[] key) {
        byte[] value;
        try {
            value = internal.get(key);
            return Optional.ofNullable(value == null ? null : ByteBuffer.wrap(value));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }
}
