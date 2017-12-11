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

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The implementing class must provide a constructor that takes two arguments:
 * Config config
 * int partition
 */
public abstract class MemStore {

    final public static String CONFIG_STORE_NAME = "name";
    final public static String CONFIG_DATA_PATH = "data.dir";

    private final static Logger log = LoggerFactory.getLogger(MemStore.class);

    protected abstract boolean isPersistent();

    private class Checkpointer extends Thread {

        final private AtomicReference<Checkpoint> checkpoint = new AtomicReference<>(new Checkpoint(-1L, 0));

        volatile private boolean checkpointModified = false;

        volatile private boolean stopped = true;

        final private Path file;

        final private boolean enabled;

        public Checkpointer(Config config, int partition) throws IOException {
            super();
            enabled = isPersistent() && config.hasPath(MemStore.CONFIG_DATA_PATH) && config.hasPath(CONFIG_STORE_NAME);
            if (!enabled) {
                file = null;
            } else {
                String dataPath = config.getString(MemStore.CONFIG_DATA_PATH);
                file = Paths.get(dataPath, "checkpoints", config.getString(CONFIG_STORE_NAME) + "-" + partition + ".checkpoint");
                if (!Files.exists(file)) {
                    log.debug("Creating checkpoint file: " + file);
                    Files.createDirectories(file.getParent());
                    writeCheckpoint();
                } else {
                    log.debug("Reading checkpoint file: " + file);
                    java.util.List<String> lines = Files.readAllLines(file);
                    checkpoint.set(new Checkpoint(Long.valueOf(lines.get(0)), Long.valueOf(lines.get(1))));
                }
                log.info("Initialized checkpoint: " + checkpoint + " from file " + file);
                start();
            }
        }

        private void writeCheckpoint() throws IOException {
            Checkpoint chk = checkpoint.get();
            log.debug("Writing checkpoint " + chk + " to file: " + file);
            Files.write(file, Arrays.asList(String.valueOf(chk.offset), String.valueOf(chk.size)));
            checkpointModified = false;
        }

        private Checkpoint updateCheckpoint(long offset, long sizeDelta) {
            return checkpoint.updateAndGet(chk -> {
                if (log.isTraceEnabled()) {
                    log.trace("updating checkpoint, offset: " + offset +", sizeDelta: " + sizeDelta);
                }
                if (offset > chk.offset) {
                    if (enabled) checkpointModified = true;
                    return new Checkpoint(offset, chk.size + sizeDelta);
                } else if (sizeDelta != 0) {
                    if (enabled) checkpointModified = true;
                    return chk.withSizeDelta(sizeDelta);
                } else {
                    return chk;
                }
            });
        }

        @Override
        public void run() {
            try {
                stopped = false;
                while(!stopped) {
                    Thread.sleep(10000); //TODO make checkpoint interval configurable
                    if (checkpointModified) {
                        writeCheckpoint();
                    }
                }
            } catch (Exception e) {
                log.error("Error in the checkpointer thread", e);
                Thread.currentThread().getThreadGroup().interrupt();
            }
        }
    }

    public MemStore(Config config, int partition) throws IOException {
        checkpointer = new Checkpointer(config, partition);
    }

    public Checkpoint getCheckpoint() {
        return checkpointer.checkpoint.get();
    }

    final private Checkpointer checkpointer;

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
        return checkpointer.checkpoint.get().size;
    }

    /**
     * @param key   ByteBuffer representation
     * @param value ByteBuffer which will be associated with the given key
     * @return new Checkpoint after the operation
     */
    public final Checkpoint put(ByteBuffer key, ByteBuffer value, long offset) {
        return checkpointer.updateCheckpoint(offset, putImpl(key, value) ? 1L : 0L);
    }

    /**
     * @param key   ByteBuffer representation
     * @param value ByteBuffer which will be associated with the given key
     * @return new Checkpoint after the operation
     */
    protected abstract boolean putImpl(ByteBuffer key, ByteBuffer value);

    /**
     * @param key ByteBuffer representation whose value will be removed
     * @return long size of the memstore (number of keys) after the operation
     */
    public final Checkpoint remove(ByteBuffer key, long offset) {
        return checkpointer.updateCheckpoint(offset, removeImpl(key) ? -1L : 0);
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
    public void close() {
        checkpointer.stopped = true;
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
        if (ttlMs < Long.MAX_VALUE && valueAndMetadata.getLong(0) + ttlMs < System.currentTimeMillis()) {
            //TODO #65 this is the only place where expired records get actually cleaned from the memstore but we need also a regular full compaction process that will get the memstore iterator and call this method
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


}
