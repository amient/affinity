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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * The implementing class must provide either no-arg constructor or a constructor that takes two arguments:
 *  Config config
 *  int partition
 */
public interface MemStore {

    Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator();

    /**
     * @param key ByteBuffer representation of the key
     * @return Some(MV) if key exists
     * None if the key doesn't exist
     */
    Optional<ByteBuffer> apply(ByteBuffer key);

    /**
     * @param key ByteBuffer representation
     * @param value ByteBuffer which will be associated with the given key
     * @return optional value held at the key position before the update
     */
    Optional<ByteBuffer> update(ByteBuffer key, ByteBuffer value);

    /**
     * @param key ByteBuffer representation whose value will be removed
     * @return optional value held at the key position before the update, None if the key doesn't exist
     */
    Optional<ByteBuffer> remove(ByteBuffer key);

    /**
     * close() will be called whenever the owning storage is closing
     * implementation should clean-up any resources here
     */
    void close();

}
