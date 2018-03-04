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

import io.amient.affinity.core.util.ByteUtils;
import io.amient.affinity.core.util.CloseableIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemStoreSortedMap extends MemStore {

    final private ConcurrentSkipListMap<ByteBuffer, ByteBuffer> internal2 = new ConcurrentSkipListMap<>();

    public MemStoreSortedMap(StateConf conf) throws IOException {
        super(conf);
    }

    @Override
    protected boolean isPersistent() {
        return false;
    }

    @Override
    public CloseableIterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator(ByteBuffer prefix) {
        if (prefix == null) {
            return CloseableIterator.apply(internal2.entrySet().iterator());
        } else {
            ByteBuffer startKey = internal2.higherKey(prefix);
            if (startKey == null) {
                return CloseableIterator.empty();
            } else {
                Iterator<Map.Entry<ByteBuffer, ByteBuffer>> tailEntries = internal2.tailMap(startKey).entrySet().iterator();
                return new CloseableIterator<Map.Entry<ByteBuffer, ByteBuffer>>() {
                    Map.Entry<ByteBuffer, ByteBuffer> head = null;
                    @Override
                    public void close() throws IOException { }

                    @Override
                    public boolean hasNext() {
                        if (head == null) {
                            if (tailEntries.hasNext()) {
                                head = tailEntries.next();
                                if (!ByteUtils.startsWith(head.getKey(), prefix)) {
                                    head = null;
                                }
                            }
                        }
                        return head != null;
                    }

                    @Override
                    public Map.Entry<ByteBuffer, ByteBuffer> next() {
                        if (!hasNext()) throw new NoSuchElementException();
                        Map.Entry<ByteBuffer, ByteBuffer> result = head;
                        head = null;
                        return result;
                    }
                };
            }
        }
    }

    @Override
    public Optional<ByteBuffer> apply(ByteBuffer key) {
        return Optional.ofNullable(internal2.get(key));
    }

    @Override
    public long numKeys() {
        return internal2.size();
    }

    @Override
    public void put(ByteBuffer key, ByteBuffer value) {
        internal2.put(key, value);
    }

    @Override
    public void remove(ByteBuffer key) {
        internal2.remove(key);
    }

    @Override
    public void close() throws IOException {
        internal2.clear();
    }
}
