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

import com.typesafe.config.Config;
import io.amient.affinity.core.storage.JavaMemStore;
import io.amient.affinity.core.util.ByteUtils;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;


public class MemStoreRocksDb implements JavaMemStore {


    public static final String CONFIG_ROCKSDB_DATA_PATH = "memstore.rocksdb.data.path";

    private static Map<String, Long> refs = new HashMap<>();
    private static Map<String, RocksDB> instances = new HashMap<>();

    synchronized private static final RocksDB createOrGetDbInstanceRef(String pathToData, Options rocksOptions) {
        RocksDB.loadLibrary();
        if (refs.containsKey(pathToData) && refs.get(pathToData) > 0) {
            refs.put(pathToData, refs.get(pathToData) + 1);
            return instances.get(pathToData);
        } else {

            try {
                RocksDB instance = RocksDB.open(rocksOptions, pathToData);
                instances.put(pathToData, instance);
                refs.put(pathToData, 1L);
                return instance;
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        }
    }

    synchronized private static final void releaseDbInstance(String pathToData) {
        if (refs.get(pathToData) > 1) {
            refs.put(pathToData, refs.get(pathToData) - 1);
        } else {
            refs.remove(pathToData);
            instances.get(pathToData).close();
        }
    }

    private final String pathToData;
    private final Path containerPath;
    private final RocksDB internal;

    public MemStoreRocksDb(Config config, int partition) throws IOException {
        pathToData = config.getString(CONFIG_ROCKSDB_DATA_PATH) + "/" + partition + "/";
        containerPath = Paths.get(pathToData).getParent().toAbsolutePath();
        Files.createDirectories(containerPath);
        Options rocksOptions = new Options().setCreateIfMissing(true);
        internal = createOrGetDbInstanceRef(pathToData, rocksOptions);
    }

    @Override
    public Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator() {
        return new java.util.Iterator<Map.Entry<ByteBuffer, ByteBuffer>>() {
            private RocksIterator rocksIterator = null;
            private boolean checked = false;
            @Override
            public boolean hasNext() {
                checked = true;
                if (rocksIterator == null) {
                    rocksIterator = internal.newIterator();
                    rocksIterator.seekToFirst();
                } else {
                    rocksIterator.next();
                }
                return rocksIterator.isValid();
            }

            @Override
            public Map.Entry<ByteBuffer, ByteBuffer> next() {
                if (!checked) {
                    if (!hasNext()) throw new NoSuchElementException("End of iterator");
                }
                checked = false;
                return new AbstractMap.SimpleEntry<ByteBuffer, ByteBuffer>(
                        ByteBuffer.wrap(rocksIterator.key()), ByteBuffer.wrap(rocksIterator.value())
                );
            }
        };
    }

    @Override
    public Optional<ByteBuffer> apply(ByteBuffer key) {
        return get(ByteUtils.bufToArray(key));
    }

    @Override
    public Optional<ByteBuffer> update(ByteBuffer key, ByteBuffer value) {
        byte[] keyBytes = ByteUtils.bufToArray(key);
        Optional<ByteBuffer> prev = get(keyBytes);
        try {
            internal.put(keyBytes, ByteUtils.bufToArray(value));
            return prev;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<ByteBuffer> remove(ByteBuffer key) {
        byte[] keyBytes = ByteUtils.bufToArray(key);
        Optional<ByteBuffer> prev = get(keyBytes);
        try {
            internal.remove(keyBytes);
            return prev;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        releaseDbInstance(pathToData);
    }

    private Optional<ByteBuffer> get(byte[] key) {
        byte[] value = new byte[0];
        try {
            value = internal.get(key);
            return Optional.ofNullable(value == null ? null : ByteBuffer.wrap(value));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }
}
