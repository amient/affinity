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
import io.amient.affinity.core.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
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
 * String identifier
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

    final private CheckpointManager manager;

    final private boolean checkpointsEnable;
    final protected int ttlSecs;
    final protected Path dataDir;

    public MemStore(String identifier, StateConf conf) throws IOException {
        checkpointsEnable = isPersistent();
        ttlSecs = conf.TtlSeconds.apply();
        if (!checkpointsEnable) {
            dataDir = null;
        } else {
            if (!conf.MemStore.DataDir.isDefined()) {
              throw new IllegalArgumentException(conf.MemStore.DataDir.path() + " must be provided via affinity.node config for store: " + identifier);
            }
            dataDir = conf.MemStore.DataDir.apply().resolve(Paths.get(identifier));
            if (!Files.exists(dataDir)) Files.createDirectories(dataDir);
        }
        manager = new CheckpointManager();
    }

    protected abstract boolean isPersistent();

    final public void open() {
        manager.start();
    }

    public <C> LogCheckpoint<C> getCheckpoint(LogStorage<C> forStorage) {
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
     * Store value with checkpointing semantics
     * @param key       ByteBuffer representation
     * @param value     ByteBuffer which will be associated with the given key
     * @param position    checkpoint position
     * @return new Checkpoint after the operation
     */
    public final LogCheckpoint put(ByteBuffer key, ByteBuffer value, Object position) {
        putImpl(key, value);
        return manager.updateCheckpoint(position);
    }

    /**
     * Store value without checkpointing semantics
     * @param key
     * @param value
     */
    public final void put(ByteBuffer key, ByteBuffer value) {
        putImpl(key, value);
    }

    /**
     * @param key   ByteBuffer representation
     * @param value ByteBuffer which will be associated with the given key
     */
    protected abstract void putImpl(ByteBuffer key, ByteBuffer value);

    /**
     * remove key with checkpointing semantics
     * @param key ByteBuffer representation whose value will be removed
     * @return new Checkpoint valid after the operation
     * @param position  checkpoint position in the related log stream
     */
    public final LogCheckpoint remove(ByteBuffer key, Object position) {
        removeImpl(key);
        return manager.updateCheckpoint(position);
    }

    /**
     * remove key without checkpointing semantics
     * @param key ByteBuffer representation whose value will be removed
     * @return new Checkpoint valid after the operation
     */
    public final void remove(ByteBuffer key) {
        removeImpl(key);
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
     * @param position checkpoint position in the related log stream
     */
    final public void unload(byte[] key, Object position) {
        remove(ByteBuffer.wrap(key), position);
    }

    /**
     * boostrapping methods: unload()
     *
     * @param key       record key to be loaded
     * @param value     record value to be wrapped
     * @param position    checkpoint position in the related log stream
     * @param timestamp event time to be wrapped
     */
    final public void load(byte[] key, byte[] value, Object position, long timestamp) {
        ByteBuffer valueBuffer = wrap(value, timestamp);
        put(ByteBuffer.wrap(key), valueBuffer, position);
    }

    private class CheckpointManager extends Thread implements Closeable {

        final private AtomicReference<LogCheckpoint> checkpoint = new AtomicReference<>(new LogCheckpoint());

        volatile private boolean checkpointModified = false;

        volatile private boolean stopped = true;

        private Path getFile() {
            return dataDir.resolve(MemStore.this.getClass().getSimpleName() + ".checkpoint");
        }

        @Override
        public synchronized void start() {

            if (checkpointsEnable) try {
                Path file = getFile();
                if (Files.exists(file)) checkpoint.set(LogCheckpoint.readFromFile(file));
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
                        writeCheckpoint();
                    }
                }
            } catch (Exception e) {
                log.error("Error in the manager thread", e);
                Thread.currentThread().getThreadGroup().interrupt();
            }
        }

        private void writeCheckpoint() throws IOException {
            Path file = getFile();
            LogCheckpoint chk = checkpoint.get();
            log.debug("Writing checkpoint " + chk + " to file: " + file);
            chk.writeToFile(file);
            checkpointModified = false;
        }

        private LogCheckpoint updateCheckpoint(Object position) {
            return checkpoint.updateAndGet(chk -> {
                if (log.isTraceEnabled()) {
                    log.trace("updating checkpoint, offset: " + position);
                }
                if (checkpointsEnable) checkpointModified = true;
                return new LogCheckpoint(position);
            });
        }


        public void close() throws IOException {
            if (checkpointsEnable) writeCheckpoint();
        }
    }


}
