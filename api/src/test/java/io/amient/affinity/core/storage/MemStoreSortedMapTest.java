package io.amient.affinity.core.storage;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class MemStoreSortedMapTest {

    @Test
    public void shouldAllowPrefixSeek() throws IOException {

        StateConf template = new StateConf();
        Config config = ConfigFactory.empty()
                .withValue(template.MemStore.Class.path(), ConfigValueFactory.fromAnyRef(MemStoreSortedMap.class.getName()));

        MemStore instance = new MemStoreSortedMap(new StateConf().apply(config));
        try {
            instance.put(ByteBuffer.wrap("key1-A".getBytes()), ByteBuffer.wrap("value1A".getBytes()));
            instance.put(ByteBuffer.wrap("key1-B".getBytes()), ByteBuffer.wrap("value1B".getBytes()));
            instance.put(ByteBuffer.wrap("key1-C".getBytes()), ByteBuffer.wrap("value1C".getBytes()));
            instance.put(ByteBuffer.wrap("key2-A".getBytes()), ByteBuffer.wrap("value2A".getBytes()));
            instance.put(ByteBuffer.wrap("key3-A".getBytes()), ByteBuffer.wrap("value3A".getBytes()));
            instance.put(ByteBuffer.wrap("key3-B".getBytes()), ByteBuffer.wrap("value3B".getBytes()));

            Map<ByteBuffer, ByteBuffer> it = instance.applyPrefix(ByteBuffer.wrap("key1".getBytes()));
            assert(it.size() == 3);
            Map<ByteBuffer, ByteBuffer> it2 = instance.applyPrefix(ByteBuffer.wrap("key3".getBytes()));
            assert(it2.size() == 2);
        } finally {
            instance.close();
        }

    }
}
