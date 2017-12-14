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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.Future;

public abstract class Storage {

    final public static String CONFIG_MEMSTORE_CLASS = "memstore.class";

    final public MemStore memstore;

    public Storage(Config config, int partition)
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Class<? extends MemStore> memstoreClass
                = Class.forName(config.getString(CONFIG_MEMSTORE_CLASS)).asSubclass(MemStore.class);
        Constructor<? extends MemStore> memstoreConstructor = memstoreClass.getConstructor(Config.class, int.class);
        memstore = memstoreConstructor.newInstance(config, partition);
        memstore.open();
    }

    /**
     * the contract of this method is that it should start a background process of restoring
     * the state from the underlying storage.
     */
    abstract public void init();

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
     * close all resource and background processes used by the memstore and the storage implementation
     */
    final public void close() {
        try {
            stop();
        } finally {
            memstore.close();
        }
    }

    /**
     * close all resource and background processes used by the the storage implementation
     */
    abstract protected void stop();

    /**
     * @param key       record key
     * @param value     record value
     * @param timestamp logical event time
     * @return Future with long offset/audit increment
     */
    abstract public Future<Long> write(byte[] key, byte[] value, long timestamp);

    abstract public Future<Long> delete(byte[] key);

}
