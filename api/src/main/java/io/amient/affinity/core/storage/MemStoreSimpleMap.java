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

package io.amient.affinity.core.storage;

import io.amient.affinity.core.util.CloseableIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class MemStoreSimpleMap extends MemStore {

    final private ConcurrentHashMap<ByteBuffer, ByteBuffer> internal = new ConcurrentHashMap<>();

    public MemStoreSimpleMap(StateConf conf) throws IOException {
        super(conf);
    }

    @Override
    protected boolean isPersistent() {
        return false;
    }

    @Override
    public CloseableIterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator() {
        return CloseableIterator.apply(internal.entrySet().iterator());
    }

    @Override
    public Optional<ByteBuffer> apply(ByteBuffer key) {
        return Optional.ofNullable(internal.get(key));
    }

    @Override
    public long numKeys() {
        return internal.size();
    }

    @Override
    public void put(ByteBuffer key, ByteBuffer value) {
        internal.put(key, value);
    }

    @Override
    public void remove(ByteBuffer key) {
        internal.remove(key);
    }

    @Override
    public void close() throws IOException {
        internal.clear();
    }
}
