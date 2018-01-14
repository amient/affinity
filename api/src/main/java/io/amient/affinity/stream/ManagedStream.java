package io.amient.affinity.stream;

import com.typesafe.config.Config;

import java.io.Closeable;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.function.Function;

/**
 * ManagedStream is used by GatewayStream, Repartitioner Tool and CompactRDD
 * One of the kafka modules must be included that provides io.amient.affinity.stream.ManagedConsumerImpl
 */
public interface ManagedStream extends Closeable {

    static Class<? extends ManagedStream> bindClass() throws ClassNotFoundException {
        Class<?> cls = Class.forName("io.amient.affinity.stream.ManagedStreamImpl");
        return cls.asSubclass(ManagedStream.class);
    }

    static ManagedStream bindNewInstance(Config config)
            throws ClassNotFoundException,
            NoSuchMethodException, IllegalAccessException,
            InvocationTargetException, InstantiationException {
        return bindClass().getConstructor(Config.class).newInstance(config);
    }

    int getNumPartitions(String topic);

    void subscribe(String topic);

    void subscribe(String topic, int partition);

    long lag();

    Iterator<Record<byte[], byte[]>> fetch(long minTimestamp);

    void commit();

    void publish(String topic, Iterator<PartitionedRecord<byte[], byte[]>> iter, Function<Long, Boolean> checker);

    default Iterator<Record<byte[], byte[]>> iterator() {
        return new Iterator<Record<byte[], byte[]>>() {

            private Record<byte[], byte[]> record = null;
            private Iterator<Record<byte[], byte[]>> i = null;

            @Override
            public boolean hasNext() {
                if (i == null) seek();
                return record != null;
            }

            @Override
            public Record<byte[], byte[]> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                } else {
                    Record<byte[], byte[]> result = record;
                    seek();
                    return result;
                }
            }

            void seek() {
                record = null;
                while (i == null || !i.hasNext()) {
                    if (lag() <= 0) return;
                    i = fetch(-1L);
                }
                if (i.hasNext()) record = i.next();
            }

        };
    }


}
