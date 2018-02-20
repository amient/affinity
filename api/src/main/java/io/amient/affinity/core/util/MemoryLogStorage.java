package io.amient.affinity.core.util;

import io.amient.affinity.core.storage.LogEntry;
import io.amient.affinity.core.storage.LogStorage;
import io.amient.affinity.core.storage.LogStorageConf;
import io.amient.affinity.core.storage.Record;
import io.amient.affinity.core.util.EventTime;
import io.amient.affinity.core.util.TimeRange;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This is a testing utility that emulates a real LogStorage using in-memory structures
 */
public class MemoryLogStorage implements LogStorage<Long> {

    private AtomicLong logEndOffset = new AtomicLong(-1L);
    private AtomicLong fetchStopOffset = new AtomicLong(Long.MAX_VALUE);
    private ConcurrentLinkedQueue<WriteFuture> unflushedWrites = new ConcurrentLinkedQueue<>();
    private ConcurrentHashMap<Long, LogEntry<Long>> internal = new ConcurrentHashMap<>();
    private Long position = 0L;
    private AtomicBoolean cancelled = new AtomicBoolean(false);


    public MemoryLogStorage(LogStorageConf conf) { }

    @Override
    public int getNumPartitions() {
        return 1;
    }

    @Override
    public void reset(TimeRange range) {
        position = 0L;
        LogEntry<Long> b;
        while(true) {
            b = internal.get(position);
            if (b == null || b.timestamp >= range.start) break;
            position +=1;
        }
        fetchStopOffset.set(logEndOffset.get());
        while(true) {
            b = internal.get(fetchStopOffset.get());
            if (b == null || b.timestamp < range.end) {
                break;
            } else {
                fetchStopOffset.decrementAndGet();
            }
        }
        System.out.println("Reset time range: " + range + ", offsets: " + position + " .. " + fetchStopOffset);
    }

    @Override
    public void reset(int partition, TimeRange range) {
        reset(range);
    }

    @Override
    public void reset(int partition, Long startPosition) {
        position = startPosition == null ? 0 : startPosition;
        fetchStopOffset.set(logEndOffset.get());
    }

    @Override
    public Iterator<LogEntry<Long>> fetch(boolean unbounded) throws InterruptedException {
        if (!unbounded && position > fetchStopOffset.get()) return null;
        while (position > logEndOffset.get()) {
            synchronized(cancelled) {
                cancelled.wait(100);
                if (cancelled.get()) {
                    return null;
                }
            }
        }
        LogEntry<Long> result = internal.get(position);
        position += 1;
        return Collections.singletonList(result).iterator();
    }

    @Override
    public boolean isTombstone(LogEntry<Long> entry) {
        return entry.value == null;
    }

    @Override
    public void cancel() {
        cancelled.set(true);
        synchronized(cancelled) {
            cancelled.notifyAll();
        }
    }

    @Override
    public void commit() {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public Future<Long> append(Record<byte[], byte[]> record) {
        WriteFuture f = new WriteFuture(record);
        unflushedWrites.add(f);
        return f;
    }

    @Override
    public Future<Long> delete(byte[] key) {
        return append(new Record<>(key, null, EventTime.unix()));
    }

    @Override
    public void flush() {
        while (!unflushedWrites.isEmpty()) {
            WriteFuture write = unflushedWrites.poll();
            logEndOffset.updateAndGet((long pos) -> {
                long newPos = pos + 1;
                LogEntry entry = new LogEntry(newPos, write.record.key, write.record.value, write.record.timestamp);
                internal.put(newPos, entry);
                write.complete(newPos);
                return newPos;
            });
        }
    }

    @Override
    public void close() throws IOException { }


    @Override
    public String keySubject() {
        return null;
    }

    @Override
    public String valueSubject() {
        return null;
    }

    @Override
    public void ensureCorrectConfiguration(long ttlMs, int numPartitions, boolean readonly) { }


    public final class WriteFuture implements Future<Long> {

        public Record<byte[], byte[]> record;

        private final CountDownLatch latch = new CountDownLatch(1);
        private Long value;

        public WriteFuture(Record<byte[], byte[]> record) {
            this.record = record;
        }

        void complete(Long result) {
            value = result;
            latch.countDown();
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return latch.getCount() == 0;
        }

        @Override
        public Long get() throws InterruptedException {
            latch.await();
            return value;
        }

        @Override
        public Long get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
            if (latch.await(timeout, unit)) {
                return value;
            } else {
                throw new TimeoutException();
            }
        }

    }
}
