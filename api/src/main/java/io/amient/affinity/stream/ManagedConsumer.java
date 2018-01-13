package io.amient.affinity.stream;

import com.typesafe.config.Config;

import java.io.Closeable;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * ManagedConsumer is used by GatewayStream either for plain input streams or repartitioner tool.
 * One of the kafka modules must be included that provides io.amient.affinity.stream.ManagedConsumerImpl
 */
public interface ManagedConsumer extends Closeable {

    static Class<? extends ManagedConsumer> bindClass() throws ClassNotFoundException {
        Class<?> cls = Class.forName("io.amient.affinity.stream.ManagedConsumerImpl");
        return cls.asSubclass(ManagedConsumer.class);
    }

    static ManagedConsumer bindNewInstance(Config config)
            throws ClassNotFoundException,
            NoSuchMethodException, IllegalAccessException,
            InvocationTargetException, InstantiationException {
        return bindClass().getConstructor(Config.class).newInstance(config);
    }

    void subscribe(String topic);

    void subscribe(String topic, int partition);

    Map<String, Long> lag();

    Iterator<Record<byte[], byte[]>> fetch(long minTimestamp);

    default long maxLag() {
        Map<String, Long> x = lag();
        return x.isEmpty() ? 0L : x.entrySet().stream().map(y -> y.getValue()).max(Long::compareTo).orElse(0L);
    }

    void commit();

    default Iterator<Record<byte[], byte[]>> iterator() {
        return new Iterator<Record<byte[], byte[]>>() {

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Record<byte[], byte[]> next() {
                return null;
            }

        };
    }
}
