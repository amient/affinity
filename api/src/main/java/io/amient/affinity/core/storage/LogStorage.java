package io.amient.affinity.core.storage;

import com.typesafe.config.Config;
import io.amient.affinity.core.util.TimeRange;

import java.io.Closeable;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Future;

/**
 * LogStorage represents a partitioned key-value stream of data
 * It is used by State, GatewayStream, Repartitioner and a CompactRDD.
 *
 * @param <P> Coordinate type that describes a position in the log stream
 */
public interface LogStorage<P extends Comparable<P>> extends Closeable {

    LogStorageConf Conf = new LogStorageConf() {
        @Override
        public LogStorageConf apply(Config config) {
            return new LogStorageConf().apply(config);
        }
    };

    static LogStorage newInstance(Config config)
            throws ClassNotFoundException,
            NoSuchMethodException,
            InvocationTargetException,
            InstantiationException,
            IllegalAccessException {
        return newInstance(LogStorage.Conf.apply(config));
    }

    static LogStorage newInstance(LogStorageConf conf)
            throws ClassNotFoundException,
            NoSuchMethodException, IllegalAccessException,
            InvocationTargetException, InstantiationException {

        Class<? extends LogStorage> cls = conf.Class.apply();
        return cls.getConstructor(LogStorageConf.class).newInstance(conf);
    }

    default Log<P> createManager(Path checkpointFile, int partition, boolean enabled) {
        Log<P> instance = new Log<>(this, checkpointFile, enabled);
        P checkpoint = instance.getCheckpoint();
        if (checkpoint != null) {
            seek(partition, checkpoint);
        }
        return instance;
    }

    int getNumPartitions();

    void reset(TimeRange range);

    void reset(int partition, TimeRange range);

    void seek(int partition, P position);

    String keySubject();

    String valueSubject();

    void ensureCorrectConfiguration(long ttlMs, int numPartitions, boolean readonly);

    /**
     *
     * @return iterator of records which may be empty, or null if the maximum offset was reached
     */
    Iterator<LogEntry<P>> fetch() throws InterruptedException;

    void commit();

    Future<P> append(Record<byte[], byte[]> record);

    Future<P> delete(byte[] key);

    void flush();

    default Iterator<LogEntry<P>> iterator() {
        return new Iterator<LogEntry<P>>() {

            private LogEntry<P> record = null;
            private Iterator<LogEntry<P>> i = null;

            @Override
            public boolean hasNext() {
                if (i == null) seek();
                return record != null;
            }

            @Override
            public LogEntry<P> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                } else {
                    LogEntry<P> result = record;
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
