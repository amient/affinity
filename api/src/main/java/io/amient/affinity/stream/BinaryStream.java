package io.amient.affinity.stream;

import com.typesafe.config.Config;
import io.amient.affinity.core.Murmur2Partitioner;
import io.amient.affinity.core.Partitioner;
import io.amient.affinity.core.storage.Storage;
import io.amient.affinity.core.util.TimeRange;

import java.io.Closeable;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.NoSuchElementException;

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
            NoSuchMethodException,
            InvocationTargetException,
            InstantiationException,
            IllegalAccessException {
        return bindNewInstance(Storage.Conf.apply(config));
    }

    static BinaryStream bindNewInstance(Storage.StorageConf conf)
            throws ClassNotFoundException,
            NoSuchMethodException, IllegalAccessException,
            InvocationTargetException, InstantiationException {
        return bindClass().getConstructor(Storage.StorageConf.class).newInstance(conf);
    }

    default Partitioner getDefaultPartitioner()  {
        return new Murmur2Partitioner();
    }

    int getNumPartitions();

    void subscribe(long minTimestamp);

    void scan(int partition, TimeRange range);

    /**
     *
     * @return iterator of records which may be empty, or null if the maximum offset was reached
     */
    Iterator<BinaryRecord> fetch();

    void commit();

    long publish(Iterator<BinaryRecord> iter);

    void flush();

    default Iterator<BinaryRecord> iterator() {
        return new Iterator<BinaryRecord>() {

            private BinaryRecord record = null;
            private Iterator<BinaryRecord> i = null;

            @Override
            public boolean hasNext() {
                if (i == null) seek();
                return record != null;
            }

            @Override
            public BinaryRecord next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                } else {
                    BinaryRecord result = record;
                    seek();
                    return result;
                }
            }

            void seek() {
                record = null;
                while (i == null || !i.hasNext()) {
                    i = fetch();
                    if (i == null) return;
                }
                if (i.hasNext()) record = i.next();
            }

        };
    }


}
