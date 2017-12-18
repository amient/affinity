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

import io.amient.affinity.core.config.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The implementing class must provide a constructor that takes two arguments:
 * Config config
 * int partition
 */
public abstract class MemStore {

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

    final private MemStoreManager manager;

    final private boolean checkpointsEnable;
    final protected int ttlSecs;
    final protected Path dataDir;

    public MemStore(StateConf conf) throws IOException {
        checkpointsEnable = isPersistent() && conf.Name.isDefined();
        ttlSecs = conf.TtlSeconds.apply();
        if (!checkpointsEnable) {
            dataDir = null;
        } else {
            if (!conf.MemStore.DataDir.isDefined()) {
              throw new IllegalArgumentException(conf.MemStore.DataDir.path() + " must be provided via Node config");
            }
            dataDir = conf.MemStore.DataDir.apply().resolve(Paths.get(conf.Name.apply()));
            if (!Files.exists(dataDir)) Files.createDirectories(dataDir);
        }
        manager = new MemStoreManager();
    }

    protected abstract boolean isPersistent();

    final public void open() {
        manager.start();
    }

    public Checkpoint getCheckpoint() {
        return manager.checkpoint.get();
    }

    public abstract CloseableIterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator();

    /**
     * @param key ByteBuffer representation of the key
     * @return Some(MV) if key exists
     * None if the key doesn't exist
     */
    public abstract Optional<ByteBuffer> apply(ByteBuffer key);

    /**
     * This may or may not be accurate, depending on the underlying backend's features
     * @return number of keys in the store
     */
    public abstract long numKeys();

    /**
     * @param key       ByteBuffer representation
     * @param value     ByteBuffer which will be associated with the given key
     * @param offset    checkpoint offset
     * @return new Checkpoint after the operation
     */
    public final Checkpoint put(ByteBuffer key, ByteBuffer value, long offset) {
        putImpl(key, value);
        return manager.updateCheckpoint(offset);
    }

    /**
     * @param key   ByteBuffer representation
     * @param value ByteBuffer which will be associated with the given key
     */
    protected abstract void putImpl(ByteBuffer key, ByteBuffer value);

    /**
     * @param key ByteBuffer representation whose value will be removed
     * @return new Checkpoint valid after the operation
     * @param offset    checkpoint offset
     */
    public final Checkpoint remove(ByteBuffer key, long offset) {
        removeImpl(key);
        return manager.updateCheckpoint(offset);
    }

    /**
     * @param key key to remove
     */
    protected abstract void removeImpl(ByteBuffer key);


    /**
     * close() will be called whenever the owning storage is closing
     * implementation should clean-up any resources here
     */
    public void close() {
        try {
            manager.close();
        } catch (IOException e) {
            log.error("Failed to write final checkpoint", e);
        }
        manager.stopped = true;
    }


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
     * @return unwrapped byte array of the raw value without metadata if not expired, otherwise none
     */
    final public Optional<byte[]> unwrap(ByteBuffer key, ByteBuffer valueAndMetadata, long ttlMs) {
        if (ttlMs > 0 && valueAndMetadata.getLong(0) + ttlMs < System.currentTimeMillis()) {
            //this is the magic that expires key-value pairs based on their create timestamp
            //State.iterator also invokes unwrap for each entry therefore simply iterating cleans up expired entries
            removeImpl(key);
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
     *
     * @param key    record key
     * @param offset checkpoint offset
     */
    final public void unload(byte[] key, long offset) {
        remove(ByteBuffer.wrap(key), offset);
    }

    /**
     * boostrapping methods: unload()
     *
     * @param key       record key to be loaded
     * @param value     record value to be wrapped
     * @param offset    checkpoint offset
     * @param timestamp event time to be wrapped
     */
    final public void load(byte[] key, byte[] value, long offset, long timestamp) {
        ByteBuffer valueBuffer = wrap(value, timestamp);
        put(ByteBuffer.wrap(key), valueBuffer, offset);
    }

    private class MemStoreManager extends Thread {

        final private AtomicReference<Checkpoint> checkpoint = new AtomicReference<>(new Checkpoint(-1L, false));

        volatile private boolean checkpointModified = false;

        volatile private boolean stopped = true;

        private Path getFile() {
            return dataDir.resolve(MemStore.this.getClass().getSimpleName() + ".checkpoint");
        }

        @Override
        public synchronized void start() {

            if (checkpointsEnable) try {
                Path file = getFile();
                if (Files.exists(file)) checkpoint.set(Checkpoint.readFromFile(file));
//TODO #80 use something similar as below to implement the cleaner, but as part of the run() method
//                if (!checkpoint.get().closed) {
//                    if (!Files.exists(file)) {
//                        log.info("MemStore checkpoint doesn't exits: " + file);
//                        Files.createDirectories(file.getParent());
//                    } else {
//                        log.info("MemStore was not closed cleanly, fixing it...");
//                    }
//                    int actualSize = 0;
//                    Iterator<Map.Entry<ByteBuffer, ByteBuffer>> i = iterator();
//                    log.info("Checking records...");
//                    while (i.hasNext()) {
//                        actualSize += 1;
//                        if (actualSize % 100000 == 0) {
//                            log.info("Checked num. records: " + actualSize / 10000 + "k");
//                        }
//                        i.next();
//                    }
//                    log.info("Actual num. records: " + actualSize);
//                    log.info("Implementation num. records: " + numKeys());
//                }
                log.info("Initialized " + checkpoint + " from " + file);

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            super.start();
        }

        @Override
        public void run() {
            try {
                stopped = false;
                while (!stopped) {
                    Thread.sleep(10000); //TODO make checkpoint interval configurable
                    if (checkpointsEnable && checkpointModified) {
                        writeCheckpoint(false);
                    }
                }
            } catch (Exception e) {
                log.error("Error in the manager thread", e);
                Thread.currentThread().getThreadGroup().interrupt();
            }
        }

        private void writeCheckpoint(boolean closing) throws IOException {
            Path file = getFile();
            Checkpoint chk = (!closing) ? checkpoint.get()
                    : checkpoint.updateAndGet(c -> new Checkpoint(c.offset, true));
            log.debug("Writing checkpoint " + chk + " to file: " + file + ", final: " + closing);
            chk.writeToFile(file);
            checkpointModified = false;
        }

        private Checkpoint updateCheckpoint(long offset) {
            return checkpoint.updateAndGet(chk -> {
                if (log.isTraceEnabled()) {
                    log.trace("updating checkpoint, offset: " + offset);
                }
                if (offset > chk.offset) {
                    if (checkpointsEnable) checkpointModified = true;
                    return new Checkpoint(offset, false);
                } else {
                    return chk;
                }
            });
        }


        public void close() throws IOException {
            if (checkpointsEnable) writeCheckpoint(true);
        }
    }


}
