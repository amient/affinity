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
import io.amient.affinity.core.config.CfgStruct;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Future;

public abstract class Storage {

    public static class StorageConf extends CfgStruct<StorageConf> {
        public CfgCls<Storage> Class = cls("class", Storage.class, true);

        @Override
        protected Set<String> specializations() {
            return new HashSet<>(Arrays.asList("kafka"));
        }
    }

    final public MemStore memstore;

    public Storage(StateConf conf) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Class<? extends MemStore> memstoreClass = conf.MemStore.Class.apply();
        Constructor<? extends MemStore> memstoreConstructor = memstoreClass.getConstructor(StateConf.class);
        memstore = memstoreConstructor.newInstance(conf);
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
