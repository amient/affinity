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

import com.typesafe.config.Config;
import io.amient.affinity.core.storage.MemStore;
import io.amient.affinity.core.util.ByteUtils;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

public class MemStoreMapDb extends MemStore {

    public static final String CONFIG_MAPDB_DATA_PATH = "memstore.mapdb.data.path";
    public static final String CONFIG_MAPDB_MMAP_ENABLED = "memstore.mapdb.mmap.enabled";
    //on 64-bit systems memory mapped files can be enabled

    private static Map<String, Long> refs = new HashMap<>();
    private static Map<String, DB> instances = new HashMap<>();
    private static Map<String, ConcurrentMap<byte[], byte[]>> maps = new HashMap<>();

    synchronized private static final ConcurrentMap<byte[], byte[]> createOrGetDbInstanceRef(String pathToData, Boolean mmap) {
        if (refs.containsKey(pathToData) && refs.get(pathToData) > 0) {
            refs.put(pathToData, refs.get(pathToData) + 1);
            return maps.get(pathToData);
        } else {
            DBMaker.Maker dbMaker = DBMaker.fileDB(pathToData).checksumHeaderBypass();
            DB instance = mmap ? dbMaker.fileMmapEnable().make() : dbMaker.make();
            ConcurrentMap<byte[], byte[]> map = instance.hashMap("map", Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY)
                    .createOrOpen();
            instances.put(pathToData, instance);
            maps.put(pathToData, map);
            refs.put(pathToData, 1L);
            return map;
        }
    }

    synchronized private static final void releaseDbInstance(String pathToData) {
        if (refs.getOrDefault(pathToData, 0L) > 1) {
            refs.put(pathToData, refs.get(pathToData) - 1);
        } else {
            refs.remove(pathToData);
            instances.get(pathToData).close();
            maps.remove(pathToData);
        }
    }

    private final String pathToData;
    private final Path containerPath;
    private final ConcurrentMap<byte[], byte[]> internal;

    @Override
    protected boolean isPersistent() {
        return true;
    }

    public MemStoreMapDb(Config config, int partition) throws IOException {
        super(config, partition);
        pathToData = config.getString(CONFIG_MAPDB_DATA_PATH) + "/" + partition + "/";
        containerPath = Paths.get(pathToData).getParent().toAbsolutePath();
        Boolean mmapEnabled = config.hasPath(CONFIG_MAPDB_MMAP_ENABLED) && config.getBoolean(CONFIG_MAPDB_MMAP_ENABLED);
        Files.createDirectories(containerPath);
        this.internal = createOrGetDbInstanceRef(pathToData, mmapEnabled);
    }

    @Override
    public Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator() {
        return internal.entrySet().stream().map(entry ->
                (Map.Entry<ByteBuffer, ByteBuffer>)
                        new AbstractMap.SimpleEntry<>(
                                ByteBuffer.wrap(entry.getKey()), ByteBuffer.wrap(entry.getValue()))).iterator();
    }

    @Override
    public Optional<ByteBuffer> apply(ByteBuffer key) {
        byte[] internalValue = internal.get(ByteUtils.bufToArray(key));
        return Optional.ofNullable(internalValue == null ? null : ByteBuffer.wrap(internalValue));
    }

    @Override
    public boolean putImpl(ByteBuffer key, ByteBuffer value) {
        return internal.put(ByteUtils.bufToArray(key), ByteUtils.bufToArray(value)) == null;
    }

    @Override
    public boolean removeImpl(ByteBuffer key) {
        return internal.remove(ByteUtils.bufToArray(key)) != null;
    }

    @Override
    public void close() {
        try {
            releaseDbInstance(pathToData);
        } finally {
            super.close();
        }

    }
}
