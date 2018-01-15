package io.amient.affinity.stream;

import com.typesafe.config.Config;

import java.io.Closeable;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * BinaryStream represents any stream of data which consists of binary key-value pairs.
 * It is used by GatewayStream, Repartitioner Tool and a CompactRDD.
 * One of the modules must be included that provides io.amient.affinity.stream.BinaryStreamImpl
 */
public interface BinaryStream extends Closeable {

    static Class<? extends BinaryStream> bindClass() throws ClassNotFoundException {
        Class<?> cls = Class.forName("io.amient.affinity.stream.BinaryStreamImpl");
        return cls.asSubclass(BinaryStream.class);
    }

    static BinaryStream bindNewInstance(Config config)
            throws ClassNotFoundException,
            NoSuchMethodException, IllegalAccessException,
            InvocationTargetException, InstantiationException {
        return bindNewInstance(config, config.getString("topic"));
    }

    static BinaryStream bindNewInstance(Config config, String topic)
            throws ClassNotFoundException,
            NoSuchMethodException, IllegalAccessException,
            InvocationTargetException, InstantiationException {
        return bindClass().getConstructor(Config.class, String.class).newInstance(config, topic);
    }


    int getNumPartitions();

    void subscribe();

    void subscribe(int partition);

    long lag();

    Iterator<Record<byte[], byte[]>> fetch(long minTimestamp);

    void commit();

    long publish(Iterator<PartitionedRecord<byte[], byte[]>> iter);

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
