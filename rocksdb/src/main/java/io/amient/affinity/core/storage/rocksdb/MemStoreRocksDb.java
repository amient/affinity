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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import io.amient.affinity.core.config.Cfg;
import io.amient.affinity.core.config.CfgStruct;
import io.amient.affinity.core.state.StateConf;
import io.amient.affinity.core.storage.MemStore;
import io.amient.affinity.core.util.ByteUtils;
import io.amient.affinity.core.util.CloseableIterator;
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

        public Cfg<Long> BlockSize = longint("block.size", 4 * 1024).doc("rocks db basic block size");
        public Cfg<Boolean> OptimizeFiltersForHits = bool("optimize.filters.for.hits", true, true).doc("saves memory on bloom filters at the cost of higher i/o when the key is not found");
        public Cfg<Boolean> OptimizeForPointLookup = bool("optimize.for.point.lookup", true, true).doc("keep this on if you don't need to keep the data sorted and only use Put() and Get()");

        //
        public Cfg<Boolean> AllowMmapReads = bool("allow.mmap.reads", true, false).doc("on 64-bit systems memory mapped files can be enabled");
        public Cfg<Long> BlockCacheSize = longint("cache.size.bytes", 8 * 1024 * 1024).doc("LRU cache size, if set to 0, cache will be completely turned off");
        public Cfg<Long> WriteBufferSize = longint("write.buffer.size", false).doc("sets the size of a single memtable");
        public Cfg<Integer> WriteMaxWriteBufferNumber = integer("max.write.buffers", false).doc("sets the maximum number of memtables, both active and immutable");
        public Cfg<Integer> WriteMinWriteBufferToMergeNumber = integer("min.write.buffers.to.merge", false).doc("the minimum number of memtables to be merged before flushing to storage");
        public Cfg<Boolean> AllowConcurrentMemtableWrite = bool("allow.concurrent.writes", true, false).doc("allow concurrent writes to a memtable");

        public MemStoreRocksDbConf() {
            super(MemStoreConf.class);
        }

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

    synchronized private static final boolean releaseDbInstance(Path pathToData) {
        if (!refs.containsKey(pathToData)) {
            return false;
        } else if (refs.get(pathToData) > 1) {
            refs.put(pathToData, refs.get(pathToData) - 1);
            return false;
        } else {
            refs.remove(pathToData);
            instances.get(pathToData).close();
            return true;
        }
    }

    private final Path pathToData;
    private final RocksDB internal;
    private final String identifier;
    private final MetricRegistry metrics;
    private final Long blockCacheSize;
    private final Statistics statistics;

    @Override
    public boolean isPersistent() {
        return true;
    }

    public MemStoreRocksDb(String identifier, StateConf conf, MetricRegistry metrics) throws IOException {
        super(conf);
        pathToData = dataDir.resolve(this.getClass().getSimpleName());
        log.info("Opening RocksDb MemStore: " + pathToData);
        Files.createDirectories(pathToData);
        MemStoreRocksDbConf rocksDbConf = new MemStoreRocksDbConf().apply(conf.MemStore);

        //read tuning options and prefixes
        Options rocksOptions = new Options().setCreateIfMissing(true);
        this.blockCacheSize = rocksDbConf.BlockCacheSize.apply();
        int cacheNumShardBits;
        if (conf.MemStore.KeyPrefixSize.isDefined()) {
            rocksDbConf.OptimizeForPointLookup.setValue(false); // we'll need iterators when using prefixes
            int prefixSizeInBytes = conf.MemStore.KeyPrefixSize.apply();
            rocksOptions.useCappedPrefixExtractor(prefixSizeInBytes);
            cacheNumShardBits = prefixSizeInBytes * 8;
        } else {
            cacheNumShardBits = -1;
        }
        BlockBasedTableConfig blockTableConfig = new BlockBasedTableConfig();
        if (blockCacheSize > 0) {
            blockTableConfig.setBlockCache(new LRUCache(blockCacheSize, cacheNumShardBits, true));
        } else {
            blockTableConfig.noBlockCache();
        }

        blockTableConfig.setBlockSize(rocksDbConf.BlockSize.apply());
        if (rocksDbConf.WriteBufferSize.isDefined()) rocksOptions.setWriteBufferSize(rocksDbConf.WriteBufferSize.apply());
        if (rocksDbConf.WriteMaxWriteBufferNumber.isDefined()) rocksOptions.setMaxWriteBufferNumber(rocksDbConf.WriteMaxWriteBufferNumber.apply());
        if (rocksDbConf.WriteMinWriteBufferToMergeNumber.isDefined()) rocksOptions.setMinWriteBufferNumberToMerge(rocksDbConf.WriteMinWriteBufferToMergeNumber.apply());
        rocksOptions.setAllowConcurrentMemtableWrite(rocksDbConf.AllowConcurrentMemtableWrite.apply());
        rocksOptions.setAllowMmapReads(rocksDbConf.AllowMmapReads.apply());
        rocksOptions.setTableFormatConfig(blockTableConfig);
        if (rocksDbConf.OptimizeFiltersForHits.apply()) rocksOptions.optimizeFiltersForHits();
        if (rocksDbConf.OptimizeForPointLookup.apply()) rocksOptions.optimizeForPointLookup(blockCacheSize);

        //TODO rocksOptions.compressionType() //default snappy
        //TODO rocksOptions.setBloomLocality()
        //TODO rocksOptions.setMemtablePrefixBloomSizeRatio()
        //TODO rocksOptions.setLevel0FileNumCompactionTrigger()

        this.statistics = new Statistics();
        rocksOptions.setStatistics(statistics);
        internal = createOrGetDbInstanceRef(pathToData, rocksOptions, ttlSecs);
        this.metrics = metrics;
        this.identifier = identifier;
        if (metrics != null) {
            metrics.register("state." + identifier + ".rocksdb.index.size", new Gauge<Long>() {
                @Override
                public Long getValue() {
                    try {
                        return internal.getLongProperty("rocksdb.estimate-table-readers-mem");
                    } catch (RocksDBException e) {
                        log.warn("Could not read rocksdb.estimate-table-readers-mem property", e);
                        return 0L;
                    }
                }
            });

            metrics.register("state." + identifier + ".rocksdb.memtable.size", new Gauge<Long>() {
                @Override
                public Long getValue() {
                    try {
                        return internal.getLongProperty("rocksdb.cur-size-all-mem-tables");
                    } catch (RocksDBException e) {
                        log.warn("Could not read rocksdb.cur-size-all-mem-tables property", e);
                        return 0L;
                    }
                }
            });

            if (blockCacheSize > 0) metrics.register("state." + identifier + ".rocksdb.blockcache.size", new Gauge<Long>() {
                @Override
                public Long getValue() {
                    try {
                        return internal.getLongProperty("rocksdb.block-cache-usage");
                    } catch (RocksDBException e) {
                        log.warn("Could not read rocksdb.block-cache-usage property", e);
                        return 0L;
                    }
                }
            });
        }
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
    public String getStats() {
        return statistics.toString();
    }

    @Override
    public void close() throws IOException {
        if (releaseDbInstance(pathToData)) {
            if (metrics != null) {
                metrics.remove("state." + identifier + ".rocksdb.index.size");
                metrics.remove("state." + identifier + ".rocksdb.memtable.size");
                if (blockCacheSize > 0) metrics.remove("state." + identifier + ".rocksdb.blockcache.size");
            }
        }
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
