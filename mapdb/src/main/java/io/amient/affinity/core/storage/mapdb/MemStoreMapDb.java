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

package io.amient.affinity.core.storage.mapdb;

import io.amient.affinity.core.config.CfgBool;
import io.amient.affinity.core.config.CfgStruct;
import io.amient.affinity.core.util.CloseableIterator;
import io.amient.affinity.core.storage.MemStore;
import io.amient.affinity.core.storage.StateConf;
import io.amient.affinity.core.util.ByteUtils;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

public class MemStoreMapDb extends MemStore {

    private final static org.slf4j.Logger log = LoggerFactory.getLogger(MemStoreMapDb.class);

    public static class MemStoreMapDbConf extends CfgStruct<MemStoreMapDbConf> {

        public MemStoreMapDbConf() {
            super(MemStoreConf.class);
        }
        //on 64-bit systems memory mapped files can be enabled
        public CfgBool MemoryMapEnabled = bool("mapdb.enabled", false);

    }

    private static Map<Path, Long> refs = new HashMap<>();
    private static Map<Path, DB> instances = new HashMap<>();
    private static Map<Path, ConcurrentMap<byte[], byte[]>> maps = new HashMap<>();

    synchronized private static final ConcurrentMap<byte[], byte[]> createOrGetDbInstanceRef(
            Path pathToData, Boolean mmap, int ttlMs) {
        if (refs.containsKey(pathToData) && refs.get(pathToData) > 0) {
            refs.put(pathToData, refs.get(pathToData) + 1);
            return maps.get(pathToData);
        } else {
            DBMaker.Maker dbMaker = DBMaker.fileDB(pathToData.toFile()).checksumHeaderBypass();
            DB instance = mmap ? dbMaker.fileMmapEnable().make() : dbMaker.make();
            ConcurrentMap<byte[], byte[]> map = instance
                    .hashMap("map", Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY)
                    .expireAfterCreate(ttlMs)
                    .createOrOpen();
            instances.put(pathToData, instance);
            maps.put(pathToData, map);
            refs.put(pathToData, 1L);
            return map;
        }
    }

    synchronized private static final void releaseDbInstance(Path pathToData) {
        if (!refs.containsKey(pathToData)) {
            return;
        } else if (refs.getOrDefault(pathToData, 0L) > 1) {
            refs.put(pathToData, refs.get(pathToData) - 1);
        } else {
            refs.remove(pathToData);
            instances.get(pathToData).close();
            maps.remove(pathToData);
        }
    }

    private final Path pathToData;
    private final ConcurrentMap<byte[], byte[]> internal;

    @Override
    protected boolean isPersistent() {
        return true;
    }

    public MemStoreMapDb(StateConf conf) throws IOException {
        super(conf);
        MemStoreMapDbConf config = new MemStoreMapDbConf().apply(conf.MemStore);
        pathToData = dataDir.resolve(this.getClass().getSimpleName() + ".data");
        log.info("Opening MapDb MemStore: " + pathToData);
        Files.createDirectories(pathToData.getParent());
        Boolean mmapEnabled = config.MemoryMapEnabled.isDefined() && config.MemoryMapEnabled.apply();
        this.internal = createOrGetDbInstanceRef(pathToData, mmapEnabled, ttlSecs * 1000);
    }

    @Override
    public long numKeys() {
        return internal.size();
    }

    @Override
    public CloseableIterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator(ByteBuffer prefix) {
        return CloseableIterator.apply(internal.entrySet().stream().map(entry ->
                (Map.Entry<ByteBuffer, ByteBuffer>)
                        new AbstractMap.SimpleEntry<>(
                                ByteBuffer.wrap(entry.getKey()), ByteBuffer.wrap(entry.getValue()))).iterator());
    }

    @Override
    public Optional<ByteBuffer> apply(ByteBuffer key) {
        byte[] internalValue = internal.get(ByteUtils.bufToArray(key));
        return Optional.ofNullable(internalValue == null ? null : ByteBuffer.wrap(internalValue));
    }

    @Override
    public void put(ByteBuffer key, ByteBuffer value) {
        internal.put(ByteUtils.bufToArray(key), ByteUtils.bufToArray(value));
    }

    @Override
    public void remove(ByteBuffer key) {
        internal.remove(ByteUtils.bufToArray(key));
    }

    @Override
    public void close() {
        releaseDbInstance(pathToData);
    }
}
