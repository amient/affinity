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
import java.nio.ByteBuffer;
import java.util.concurrent.Future;

public abstract class JavaStorage {

    final public static String CONFIG_MEMSTORE_CLASS = "memstore.class";

    final public JavaMemStore memstore;

    public JavaStorage(Config config, int partition)
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Class<? extends JavaMemStore> memstoreClass
                = Class.forName(config.getString(CONFIG_MEMSTORE_CLASS)).asSubclass(JavaMemStore.class);
        JavaMemStore tmp;
        try {
            Constructor<? extends JavaMemStore> memstoreConstructor = memstoreClass.getConstructor(Config.class, int.class);
            tmp = memstoreConstructor.newInstance(config, partition);
        } catch (NoSuchMethodException e) {
            Constructor<? extends JavaMemStore> memstoreConstructor = memstoreClass.getConstructor();
            tmp = memstoreConstructor.newInstance();
        }
        memstore = tmp;
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
     *
     * Once this method returns, the partition or service which owns it will become available
     * for serving requests and so the state must be in a fully consistent state.
     *
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
     * @param key   of the pair
     * @param value of the pair
     * @return Future with metadata returned by the underlying implementation
     */
    abstract public Future<?> write(ByteBuffer key, ByteBuffer value);

}
