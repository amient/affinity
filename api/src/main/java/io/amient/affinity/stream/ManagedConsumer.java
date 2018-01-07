package io.amient.affinity.stream;

import com.typesafe.config.Config;
import io.amient.affinity.core.Record;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * ManagedConsumer is used by GatewayStream either for plain input streams or repartitioner tool.
 * One of the kafka modules must be included that provides io.amient.affinity.stream.ManagedConsumerImpl
 */
public interface ManagedConsumer extends Closeable {

    static ManagedConsumer bindNewInstance() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        return Class.forName("io.amient.affinity.stream.ManagedConsumerImpl").asSubclass(ManagedConsumer.class).newInstance();
    }

    void initialize(Config config, Set<String> topics);

    Map<String, Long> lag();

    Iterator<Record<byte[], byte[]>> fetch(long minTimestamp);

    default long maxLag() {
        Map<String, Long> x = lag();
        return x.isEmpty() ? 0L : x.entrySet().stream().map(y -> y.getValue()).max(Long::compareTo).orElse(0L);
    }

    void commit();

}
