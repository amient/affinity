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
import io.amient.affinity.core.Murmur2Partitioner;
import io.amient.affinity.core.Partitioner;
import io.amient.affinity.core.config.CfgCls;
import io.amient.affinity.core.config.CfgLong;
import io.amient.affinity.core.config.CfgStruct;
import io.amient.affinity.core.util.EventTime;
import io.amient.affinity.core.util.TimeRange;
import io.amient.affinity.stream.BinaryRecord;
import io.amient.affinity.stream.BinaryRecordAndOffset;
import io.amient.affinity.stream.BinaryStream;
import io.amient.affinity.stream.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

/**
 * All implementations must provide a single constructor with 3 parameters:
 * String identifier
 * StateConf conf
 * Int partition
 */
final public class Storage implements Closeable {

    private final static Logger log = LoggerFactory.getLogger(Storage.class);

    public static StorageConf Conf = new StorageConf() {
        @Override
        public StorageConf apply(Config config) {
            return new StorageConf().apply(config);
        }
    };

    public static class StorageConf extends CfgStruct<StorageConf> {
        public CfgCls<Storage> Class = cls("class", Storage.class, true);
        public CfgLong MinTimestamp = longint("min.timestamp.ms", 0L);

        @Override
        protected Set<String> specializations() {
            return new HashSet<>(Arrays.asList("kafka"));
        }
    }

    final private BinaryStream stream;
    final public MemStore memstore;
    final public String id;
    final public int partition;
    final long ttlMs;
    final long minTimestamp;
    final protected Partitioner defaultPartitioner = new Murmur2Partitioner();
    final Thread consumer;

    private AtomicReference<Throwable> consumerError = new AtomicReference<>(null);
    volatile private ObservableState<?> state = null;
    volatile private boolean consuming = false;
    volatile private boolean tailing = true;


    public Storage(String id, StateConf conf, int partition) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        this.partition = partition;
        this.id = id;
        Class<? extends MemStore> memstoreClass = conf.MemStore.Class.apply();
        Constructor<? extends MemStore> memstoreConstructor = memstoreClass.getConstructor(String.class, StateConf.class);
        stream = BinaryStream.bindNewInstance(conf.Storage); //TODO #115 create a concrete implementation by Storage.Class() instead of binding
        memstore = memstoreConstructor.newInstance(id, conf);
        memstore.open();
        ttlMs = conf.TtlSeconds.apply() * 1000L;
        minTimestamp = Math.max(conf.MinTimestampUnixMs.apply(), (ttlMs < 0 ? 0L : EventTime.unix() - ttlMs));
        consumer = new Thread() {
            @Override
            public void run() {
                try {
                    Checkpoint chk = memstore.getCheckpoint();
                    if (chk.offset <= 0) {
                        stream.scan(partition, new TimeRange(minTimestamp, EventTime.unix()));
                    } else {
                        stream.seek(partition, chk.offset);
                    }

                    while (true) {

                        if (isInterrupted()) throw new InterruptedException();

                        consuming = true;

                        while (consuming) {

                            if (isInterrupted()) throw new InterruptedException();

                            try {
                                Iterator<BinaryRecordAndOffset> records = stream.fetch();
                                if (records == null) {
                                    consuming = false;
                                } else while (records.hasNext()) {
                                    BinaryRecordAndOffset r = records.next();
                                //for tailing state it means either it is a) replica b) external
                                    if (r.value == null) {
                                        memstore.unload(r.key, r.offset);
                                    if (tailing) state.internalPush(r.key, Optional.empty());
                                    } else {
                                        memstore.load(r.key, r.value, r.offset, r.timestamp);
                                    if (tailing) state.internalPush(r.key, Optional.of(r.value))
                                    }
                                }
                            if (!tailing && lastProcessedOffset >= endOffset) {
                                consuming = false
                            }
                            } catch (Throwable e) {
                                synchronized (Storage.this) {
                                    consumerError.set(e);
                                    Storage.this.notify(); //boot failure
                                }
                            }
                        }

                        synchronized (Storage.this) {
                            Storage.this.notify(); //boot complete
                            Storage.this.wait(); //wait for tail instruction
                        }
                    }

                } catch (InterruptedException e) {
                    return;
                } catch (Throwable e) {
                    consumerError.set(e);
                } finally {
                    try {
                        stream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            }
        };
    }

    /**
     * the contract of this method is that it should start a background process of restoring
     * the state from the underlying storage.
     *
     * @param state observable state object that will receive notifications about updates coming externally
     *              through the storage layer
     */

    public void init(ObservableState<?> state) {
        this.state = state;
        consumer.start();
    }

    /**
     * The implementation should stop listening for updates on the underlying topic after it has
     * fully caught up with the state updates. This method must block until completed and can
     * be interrupted by a consequent tail() call.
     * <p>
     * Once this method returns, the partition or service which owns it will become available
     * for serving requests and so the state must be in a fully consistent state.
     */
    public void boot() {
//        consumer.synchronized {
//            if (tailing) {
//                tailing = false
//                while (true) {
//                    consumer.wait(1000)
//                    if (consumerError.get != null) {
//                        consumer.kafkaConsumer.wakeup()
//                        throw consumerError.get
//                    } else if (!consuming) {
//                        return
//                    }
//                }
//            }
//        }
    }

    /**
     * the implementation should start listening for updates and keep the memstore up to date.
     * The tailing must be done asychrnously - this method must return immediately and be idempotent.
     */
    public void tail() {
//        consumer.synchronized {
//            if (!tailing) {
//                tailing = true
//                consumer.notify
//            }
//        }
    }

    /**
     * close all resource and background processes used by the memstore
     * implementations of Storage should override this method and first close their specific resources
     * and then call super.close()
     */
    public void close() {
        try {
//            consumer.interrupt()
//            consumer.kafkaConsumer.wakeup()
//            if (!readonly) kafkaProducer.close
        } finally {
            try {
                memstore.close();
            } finally {
                log.debug("Closed storage " + id + ", partition: " + partition);
            }
        }
    }

    /**
     * @param record record with binary key, binary value and event timestamp
     * @return Future with long offset/audit increment
     */
    public Future<Long> write(BinaryRecord record) {
        return stream.append(record);
    }

    /**
     * @param key to be delete
     * @return Future with long offset/audit increment
     */
    public Future<Long> delete(byte[] key) {
        return stream.append(new BinaryRecord(key, null, EventTime.unix())); //TODO value=null is kafka specific operation so this needs to be delegated to the BinaryStreamImpl
    }

    /**
     * Provides subject name for schema registry
     *
     * @return subject string, null if the implementation doesn't require schema registrations
     */
    public String keySubject() {
        return stream.keySubject();
    }

    /**
     * Provides subject name for schema registry
     *
     * @return subject string, null if the implementation doesn't require schema registrations
     */
    public String valueSubject() {
        return stream.valueSubject();
    }

}
