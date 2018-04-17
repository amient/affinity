/*
 * Copyright 2016-2018 Michal Harish, michal.harish@gmail.com
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.amient.affinity.core.util;

import io.amient.affinity.core.storage.LogEntry;
import io.amient.affinity.core.storage.LogStorage;
import io.amient.affinity.core.storage.LogStorageConf;
import io.amient.affinity.core.storage.Record;

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
    private ConcurrentLinkedQueue<WritePromise> unflushedWrites = new ConcurrentLinkedQueue<>();
    private ConcurrentHashMap<Long, LogEntry<Long>> internal = new ConcurrentHashMap<>();
    private Long position = 0L;
    private AtomicBoolean cancelled = new AtomicBoolean(false);


    public MemoryLogStorage(LogStorageConf conf) { }

    @Override
    public int getNumPartitions() {
        return 1;
    }

    @Override
    public void resume(TimeRange range) {
        //N/A
    }

    @Override
    public void reset(int partition, TimeRange range) {
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
        //logger.info("Reset time range: " + range + ", offsets: " + position + " .. " + fetchStopOffset);
    }

    @Override
    public Long reset(int partition, Long startPosition) {
        position = startPosition == null ? 0 : startPosition;
        long stopOffset = logEndOffset.get();
        fetchStopOffset.set(stopOffset);
        return stopOffset;
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
    public Future<Long> commit() {
        return new CompletedJavaFuture<>(() -> System.currentTimeMillis());
    }

    @Override
    public Future<Long> append(Record<byte[], byte[]> record) {
        WritePromise f = new WritePromise(record);
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
            WritePromise write = unflushedWrites.poll();
            logEndOffset.updateAndGet((long pos) -> {
                long newPos = pos + 1;
                LogEntry entry = new LogEntry(newPos, write.record.key, write.record.value, write.record.timestamp);
                internal.put(newPos, entry);
                write.success(newPos);
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

    public final class WritePromise extends JavaPromise<Long> {
        public Record<byte[], byte[]> record;
        public WritePromise(Record<byte[], byte[]> record) {
            this.record = record;
        }
    }
}
