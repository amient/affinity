/*
 * Copyright 2016-2018 Michal Harish, michal.harish@gmail.com
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

package io.amient.affinity.core.util;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class ThreadLocalCache<K, V> extends ThreadLocal<HashMap<K, V>> {

    ConcurrentHashMap<K, V> global = new ConcurrentHashMap<>();

    @Override
    protected HashMap<K, V> initialValue() {
        return new HashMap<>();
    }

    public void initialize(K key, V value) {
        global.put(key, value);
    }

    public V getOrInitialize(K key, Supplier<V> supplier) {
        HashMap<K, V> instance = get();
        V current = instance.get(key);
        if (current != null) return current;
        V initialized = global.get(key);
        if (initialized == null) {
            initialized = supplier.get();
            global.put(key, initialized);
        }
        instance.put(key, initialized);
        return initialized;
    }

}
