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
import io.amient.affinity.core.util.CloseableIterator;
import io.amient.affinity.core.storage.MemStore;
import io.amient.affinity.core.storage.StateConf;
import io.amient.affinity.core.util.ByteUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import static org.junit.Assert.*;

public class MemStoreRocksDbTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testRocksDb() throws IOException, InterruptedException {
        String tmp = folder.newFolder().toString();

        StateConf template = new StateConf();
        Config config = ConfigFactory.empty()
                .withValue(template.MemStore.DataDir.path(), ConfigValueFactory.fromAnyRef(tmp))
                .withValue(template.MemStore.Class.path(), ConfigValueFactory.fromAnyRef(MemStoreRocksDb.class.getName()));

        MemStore instance = new MemStoreRocksDb(new StateConf().apply(config));
        try {
            ByteBuffer key1 = ByteBuffer.wrap("key1".getBytes());
            ByteBuffer key2 = ByteBuffer.wrap("key2".getBytes());
            instance.put(key1, ByteBuffer.wrap("value1".getBytes()));
            assertTrue(instance.apply(key1).isPresent());
            assertEquals("value1", new String(ByteUtils.bufToArray(instance.apply(key1).get())));
            assertTrue(!instance.apply(key2).isPresent());
            instance.put(key1, ByteBuffer.wrap("value1000".getBytes()));
            instance.put(key2, ByteBuffer.wrap("value2000".getBytes()));
            CloseableIterator<Map.Entry<ByteBuffer, ByteBuffer>> it = instance.iterator();
            assertEquals("value1000", new String(ByteUtils.bufToArray(it.next().getValue())));
            assertEquals("value2000", new String(ByteUtils.bufToArray(it.next().getValue())));
            assertFalse(it.hasNext());
            assertTrue(!it.hasNext());
            assertEquals(3, instance.numKeys());
            it.close();
        } finally {
            instance.close();
        }
    }

}
