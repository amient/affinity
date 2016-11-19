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
package io.amient.affinity.core.storage.rocksdb;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import io.amient.affinity.core.storage.JavaMemStore;
import io.amient.affinity.core.util.ByteUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MemStoreRocksDbTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testRocksDb() throws IOException, InterruptedException {
        String tmp = folder.newFolder().toString();
        Config config = ConfigFactory.empty()
                .withValue(MemStoreRocksDb.CONFIG_ROCKSDB_DATA_PATH, ConfigValueFactory.fromAnyRef(tmp));

        JavaMemStore instance = new MemStoreRocksDb(config, 0);
        try {
            ByteBuffer key1 = ByteBuffer.wrap("key1".getBytes());
            ByteBuffer key2 = ByteBuffer.wrap("key2".getBytes());
            instance.update(key1, ByteBuffer.wrap("value1".getBytes()));
            assertTrue(instance.apply(key1).isPresent());
            assertEquals("value1", new String(ByteUtils.bufToArray(instance.apply(key1).get())));
            assertTrue(!instance.apply(key2).isPresent());
            instance.update(key1, ByteBuffer.wrap("value1000".getBytes()));
            instance.update(key2, ByteBuffer.wrap("value2000".getBytes()));
            Iterator<Map.Entry<ByteBuffer, ByteBuffer>> it = instance.iterator();
            assertEquals("value1000", new String(ByteUtils.bufToArray(it.next().getValue())));
            assertEquals("value2000", new String(ByteUtils.bufToArray(it.next().getValue())));
            assertTrue(!it.hasNext());
        } finally {
            //instance.close();
        }
    }

}
