package io.amient.affinity.stream;

import com.typesafe.config.Config;
import io.amient.affinity.core.storage.Storage;
import io.amient.affinity.core.util.TimeRange;

import java.io.Closeable;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Future;

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

    int getNumPartitions();

    void subscribe(long minTimestamp);

    void scan(int partition, TimeRange range);

    void seek(int partition, long startOffset);

    String keySubject();

    String valueSubject();

    /**
     *
     * @return iterator of records which may be empty, or null if the maximum offset was reached
     */
    Iterator<BinaryRecordAndOffset> fetch() throws InterruptedException;

    void commit();

    Future<Long> append(BinaryRecord record);

    void flush();

    default Iterator<BinaryRecordAndOffset> iterator() {
        return new Iterator<BinaryRecordAndOffset>() {

            private BinaryRecordAndOffset record = null;
            private Iterator<BinaryRecordAndOffset> i = null;

            @Override
            public boolean hasNext() {
                if (i == null) seek();
                return record != null;
            }

            @Override
            public BinaryRecordAndOffset next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                } else {
                    BinaryRecordAndOffset result = record;
                    seek();
                    return result;
                }
            }

            void seek() {
                record = null;
                while (i == null || !i.hasNext()) {
                    try {
                        i = fetch();
                        if (i == null) return;
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                }
                if (i.hasNext()) record = i.next();
            }

        };
    }


}
