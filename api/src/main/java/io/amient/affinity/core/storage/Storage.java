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

import io.amient.affinity.core.Murmur2Partitioner;
import io.amient.affinity.core.Partitioner;
import io.amient.affinity.core.config.CfgCls;
import io.amient.affinity.core.config.CfgLong;
import io.amient.affinity.core.config.CfgStruct;
import io.amient.affinity.stream.BinaryRecord;
import io.amient.affinity.stream.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * All implementations must provide a single constructor with 3 parameters:
 * String identifier
 * StateConf conf
 * Int partition
 */
public abstract class Storage implements Closeable {

    private final static Logger log = LoggerFactory.getLogger(Storage.class);

    public static class StorageConf extends CfgStruct<StorageConf> {
        public CfgCls<Storage> Class = cls("class", Storage.class, true);
        public CfgLong MinTimestamp = longint("min.timestamp.ms", 0L);
        @Override
        protected Set<String> specializations() {
            return new HashSet<>(Arrays.asList("kafka"));
        }
    }

    final public MemStore memstore;
    final public String id;
    final public int partition;
    final protected Partitioner defaultPartitioner = new Murmur2Partitioner();

    public Storage(String id, StateConf conf, int partition) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        this.partition = partition;
        this.id = id;
        Class<? extends MemStore> memstoreClass = conf.MemStore.Class.apply();
        Constructor<? extends MemStore> memstoreConstructor = memstoreClass.getConstructor(String.class, StateConf.class);
        memstore = memstoreConstructor.newInstance(id, conf);
        memstore.open();
    }


    /**
     * the contract of this method is that it should start a background process of restoring
     * the state from the underlying storage.
     * @param state observable state object that will receive notifications about updates coming externally
     *              through the storage layer
     */
    abstract public void init(ObservableState<?> state);

    /**
     * The implementation should stop listening for updates on the underlying topic after it has
     * fully caught up with the state updates. This method must block until completed and can
     * be interrupted by a consequent tail() call.
     * <p>
     * Once this method returns, the partition or service which owns it will become available
     * for serving requests and so the state must be in a fully consistent state.
     */
    abstract public void boot();

    /**
     * the implementation should start listening for updates and keep the memstore up to date.
     * The tailing must be done asychrnously - this method must return immediately and be idempotent.
     */
    abstract public void tail();

    /**
     * close all resource and background processes used by the memstore
     * implementations of Storage should override this method and first close their specific resources
     * and then call super.close()
     */
    public void close() {
        try {
            memstore.close();
        } finally {
            log.debug("Closed storage " + id + ", partition: " + partition);
        }
    }

    /**
     * @param record    record with binary key, binary value and event timestamp
     * @return Future with long offset/audit increment
     */
    abstract public Future<Long> write(Record<byte[], byte[]> record);

    /**
     * @param key to be delete
     * @return Future with long offset/audit increment
     */
    abstract public Future<Long> delete(byte[] key);

    /**
     * Provides subject name for schema registry
     * @return subject string, null if the implementation doesn't require schema registrations
     */
    abstract public String keySubject();

    /**
     * Provides subject name for schema registry
     * @return subject string, null if the implementation doesn't require schema registrations
     */
    abstract public String valueSubject();

}
