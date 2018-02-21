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
 * @param <POS> Coordinate type that describes a position in the log stream
 */
public interface LogStorage<POS extends Comparable<POS>> extends Closeable {

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

    /**
     *
     * @param checkpointFile - file to be used for checkpointing or null to disable checkpointing
     * @return Log instance that
     */
    default Log<POS> open(Path checkpointFile) {
        return new Log<>(this, checkpointFile);
    }

    /**
     * get number of partitions of the underlying physical log
     * @return number of partitions
     */
    int getNumPartitions();

    /**
     * reset all of the assigned partitions to the given time range.
     * the lower bound position of each partition will be the first position which appears on the time range.start or later.
     * the upper bound position of each partition will be the last position before the time range.end OR maximum available position
     * @param range time range
     */
    void reset(TimeRange range);

    /**
     * reset one of the assigned partitions to the given time range.
     * the lower bound position will be the first position which appears on the time range.start or later.
     * the upper bound position will be the last position before the time range.end OR maximum available position
     * @param partition partition number
     * @param range time range
     */
    void reset(int partition, TimeRange range);

    /**
     * reset one of the assigned partitions to the new startPosition.
     * this reset call MUST als o set the upper bound position to the latest position available in the
     * underlying physical log, i.e. the resulting offset range should be everything from the startPosition until
     *  the end of the log.
     *
     * @param partition partition number [0 - (numPartitions-1)]
     * @param startPosition position to seek or null if resetting to the beginning of the log
     */
    void reset(int partition, POS startPosition);

    /**
     * get key subject for the schema registry
     * @return subject name of the log keys
     */
    String keySubject();

    /**
     * get value subject for the schema registry
     * @return subject name of the log values
     */
    String valueSubject();

    /**
     * ensure confiugration of the underlying physical log. This call may be called concurrently
     * by multiple instances so must be at minimum idempotent.
     *
     * @param ttlMs maximum time-to-live in milliseconds of each Record, e.g. record's timestamp + ttlMs is the time when the record expires
     * @param numPartitions - expected number of partitions of the underlying physical log
     * @param readonly - whether the log is only for reading, e.g. it is an externally produced topic
     */
    void ensureCorrectConfiguration(long ttlMs, int numPartitions, boolean readonly);

    /**
     * fetch a record batch from the underlying stream. this method should block
     * if there are no more records available and the unbounded paramtere is set to true.
     *
     * @param unbounded if false the fetch will return null when the limit set
     *                  by the reset() method is reached.
     *                  if true the fetch will block when no more records are available
     *                  even if the upper limit set by reset is reached
     *
     * @return iterator of records which may be empty, or null if the maximum offset was reached or cancel() was called
     *
     * @throws InterruptedException if the underlying blocking operation is interrupted
     */
    Iterator<LogEntry<POS>> fetch(boolean unbounded) throws InterruptedException;

    /**
     * check if an entry is a delete marker/tombstone
     * @param entry to check
     * @return true if the given entry represents a delete mark/tombstone
     */
    boolean isTombstone(LogEntry<POS> entry);


    /**
     * Cancel any ongoing blocking operation, whether fetch or iterator.hasNext etc.
     */
    void cancel();

    /**
     * Commit all positions that were advanced by one of the iterators or the underlying fetch()
     */
    void commit();

    /**
     * Append a record to the end of the log
     * @param record record to append to the log
     * @return future with the new log position checkpoint
     */
    Future<POS> append(Record<byte[], byte[]> record);

    /**
     * Append a tombstone to the end of the log for the given record key
     * @param key which will be marked as deleted
     * //TODO delete should also take a custom timestamp
     * @return future with the new log position checkpoint
     */
    Future<POS> delete(byte[] key);

    /**
     * Flush all writes that were created by append() or delete().
     * This should block until all calls to append() or delete() have completed successfully or
     * throw exception otherwise.
     */
    void flush();

    /**
     * Iterator which is bounded by the previous reset() command
     * @return iterator of log entries
     */
    default Iterator<LogEntry<POS>> boundedIterator() {
        return iterator(false);
    }

    /**
     * Iterator which is unbounded by the upper bounds given in the reset() command
     * @return iterator of log entries
     */
    default Iterator<LogEntry<POS>> unboundedIterator() {
        return iterator(true);
    }

    /**
     * General Iterator Implementation
     * @param unbounded whether the iterator is unbounded or not
     * @return iterator of log entries
     */
    default Iterator<LogEntry<POS>> iterator(boolean unbounded) {
        return new Iterator<LogEntry<POS>>() {

            private LogEntry<POS> record = null;
            private Iterator<LogEntry<POS>> i = null;

            @Override
            public boolean hasNext() {
                if (i == null) seek();
                return record != null;
            }

            @Override
            public LogEntry<POS> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                } else {
                    LogEntry<POS> result = record;
                    seek();
                    return result;
                }
            }

            void seek() {
                record = null;
                while (i == null || !i.hasNext()) {
                    try {
                        i = fetch(unbounded);
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
