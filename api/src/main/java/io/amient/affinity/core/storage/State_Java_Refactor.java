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

package io.amient.affinity.core.storage;

import io.amient.affinity.core.serde.AbstractSerde;
import io.amient.affinity.core.util.CloseableIterator;
import io.amient.affinity.core.util.CompletedJavaFuture;
import io.amient.affinity.core.util.EventTime;
import io.amient.affinity.core.util.MappedJavaFuture;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

public class State_Java_Refactor<K, V> extends ObservableState<K> implements Closeable {

    private final MemStore kvstore;
    private final Optional<Log<?>> logOption;
    private final int partition;
    private final AbstractSerde<K> keySerde;
    private final AbstractSerde<V> valueSerde;
    private final long ttlMs;
    private final int lockTimeoutMs;
    private final boolean readonly;

    public State_Java_Refactor(MemStore kvstore,
                               Optional<Log<?>> logOption,
                               int partition,
                               AbstractSerde<K> keySerde,
                               AbstractSerde<V> valueSerde,
                               long ttlMs, //-1
                               int lockTimeoutMs, //10000
                               boolean readonly //false
    ) {
        this.kvstore = kvstore;
        this.logOption = logOption;
        this.partition = partition;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.ttlMs = ttlMs;
        this.lockTimeoutMs = lockTimeoutMs;
        this.readonly = readonly;

    }

    void boot() {
        logOption.ifPresent(log -> log.bootstrap(kvstore, partition));
    }

    void tail() {
        logOption.ifPresent(log -> log.tail(kvstore, this));
    }

    public void close() throws IOException {
        try {
            if (logOption.isPresent()) logOption.get().close();
        } finally {
            kvstore.close();
        }
    }

    /**
     * @return numKeys hint - this may or may not be accurate, depending on the underlying backend's features
     */
    public Long numKeys() {
        return kvstore.numKeys();
    }

    /**
     * @return a weak iterator that doesn't block read and write operations
     */
    public CloseableIterator<Record<K, V>> iterator() {
        return new CloseableIterator<Record<K, V>>() {
            CloseableIterator<Map.Entry<ByteBuffer, ByteBuffer>> underlying = kvstore.iterator();
            private Record<K, V> current = null;

            public boolean hasNext() {
                if (current != null) return true;
                else while (underlying.hasNext()) {
                    Map.Entry<ByteBuffer, ByteBuffer> entry = underlying.next();
                    Optional<Record<byte[], byte[]>> value = kvstore.unwrap(entry.getKey(), entry.getValue(), ttlMs);
                    if (value.isPresent()) {
                        Record<byte[], byte[]> record = value.get();
                        K key = keySerde.fromBytes(record.key);
                        current = new Record<>(key, valueSerde.fromBytes(record.value), record.timestamp);
                        return true;
                    }
                }
                return false;
            }

            public Record<K, V> next() {
                if (!hasNext()) throw new NoSuchElementException();
                Record<K, V> result = current;
                current = null;
                return result;
            }

            public void close() throws IOException {
                underlying.close();
            }
        };
    }


    /**
     * Retrieve a value from the store asynchronously
     *
     * @param key to retrieve value of
     * @return Future.Success(Some(V)) if the key exists and the value could be retrieved and deserialized
     * Future.Success(None) if the key doesn't exist
     * Future.Failed(Throwable) if a non-fatal exception occurs
     */
    Optional<V> apply(K key) {
        return apply(ByteBuffer.wrap(keySerde.toBytes(key)));
    }

    private Optional<V> apply(ByteBuffer key) {
        return kvstore.apply(key).flatMap((cell) ->
                kvstore.unwrap(key, cell, ttlMs).map((byteRecord) -> valueSerde.fromBytes(byteRecord.value))
        );
    }

    /**
     * replace is a faster operation than update because it doesn't look at the existing value
     * associated with the given key
     *
     * @param key   to update
     * @param value new value to be associated with the key
     * @return Unit Future which may be failed if the operation didn't succeed
     */
    public Future<Void> replace(K key, V value) {
        return new MappedJavaFuture(put(keySerde.toBytes(key), value)) {
            @Override
            public Void map(Object result) {
                push(key, value);
                return null;
            }
        };
    }

    /**
     * delete the given key
     *
     * @param key to delete
     * @return Unit Future which may be failed if the operation didn't succeed
     */
    public Future<Void> delete(K key) {
        return new MappedJavaFuture(delete(keySerde.toBytes(key))) {
            @Override
            public Void map(Object result) {
                push(key, null);
                return null;
            }
        };
    }

    /**
     * update is a syntactic sugar for update where the value is always overriden unless the current value is the same
     *
     * @param key   to updateImpl
     * @param value new value to be associated with the key
     * @return Future Optional of the value previously held at the key position
     */
    public Future<Optional<V>> update(K key, V value) {
        return getAndUpdate(key, current -> Optional.of(value));
    }

    /**
     * remove is a is a syntactic sugar for update where None is used as Value
     * it is different from delete in that it returns the removed value
     * which is more costly.
     *
     * @param key to remove
     * @return Future optional value that was removed
     * @throws RuntimeException if any of the future failed
     */
    public Future<Optional<V>> remove(K key) {
        return getAndUpdate(key, current -> Optional.empty());
    }

    /**
     * insert is a syntactic sugar for update which is only executed if the key doesn't exist yet
     *
     * @param key   to insert
     * @param value new value to be associated with the key
     * @return Future value newly inserted if the key did not exist and operation succeeded, failed future otherwise
     * @throws RuntimeException if any of the future failed
     */
    public Future<V> insert(K key, V value) {
        return new MappedJavaFuture<Optional<V>, V>(updateAndGet(key, current -> {
            if (current.isPresent()) {
                throw new IllegalArgumentException(key + " already exists in state store");
            } else {
                return Optional.of(value);
            }
        })) {
            @Override
            public V map(Optional<V> result) {
                return result.get();
            }
        };
    }

    /**
     * This is similar to apply(key) except it also applies row lock which is useful if the client
     * wants to make sure that the returned value incorporates all changes applied to it in update operations
     * with in the same sequence.
     *
     * @param key to get value for
     * @return optional value or empty if no value is present
     * @throws TimeoutException if the lock could not be acquired within lockTimeoutMs
     * @throws InterruptedException if waiting for the lock was interrupted
     */
    public Optional<V> get(K key) throws TimeoutException, InterruptedException {
        Long l = lock(key);
        try {
            return apply(key);
        } finally {
            unlock(key, l);
        }
    }

    /**
     * atomic get-and-update if the current and updated value are the same the no modifications are made to
     * the underlying stores and the returned future is completed immediately.
     *
     * @param key   to updateImpl
     * @param f function which given a current value returns an updated value or empty if the key is to be removed
     *          as a result of the update
     * @return Future Optional of the value previously held at the key position
     */
    public Future<Optional<V>> getAndUpdate(K key, Function<Optional<V>, Optional<V>> f) {
        try {
            byte[] k = keySerde.toBytes(key);
            Long l = lock(key);
            try {
                Optional<V> currentValue = apply(ByteBuffer.wrap(k));
                Optional<V> updatedValue = f.apply(currentValue);
                if (currentValue.equals(updatedValue)) {
                    unlock(key, l);
                    return new CompletedJavaFuture<>(() -> currentValue);
                } else {
                    return new MappedJavaFuture(updatedValue.isPresent() ? put(k, updatedValue.get()) : delete(k)) {
                        @Override
                        public Optional<V> map(Object position) {
                            unlock(key, l);
                            push(key, updatedValue);
                            return currentValue;
                        }

                        @Override
                        public Boolean recover(Throwable e) throws Throwable {
                            unlock(key, l);
                            throw e;
                        }
                    };
                }
            } catch (Throwable e) {
                unlock(key, l);
                throw e;
            }
        } catch (Throwable e) {
            return new CompletedJavaFuture<>(() -> {
                throw new RuntimeException(e);
            });
        }
    }

    /**
     * atomic update-and-get - if the current and updated value are the same the no modifications are made to
     * the underlying stores and the returned future is completed immediately.
     *
     * @param key key which is going to be updated
     * @param f function which given a current value returns an updated value or empty if the key is to be removed
     *          as a result of the update
     * @return Future optional value which will be successful if the put operation succeeded and will hold the updated value
     */
    public Future<Optional<V>> updateAndGet(K key, Function<Optional<V>, Optional<V>> f) {
        try {
            byte[] k = keySerde.toBytes(key);
            Long l = lock(key);
            try {
                Optional<V> currentValue = apply(ByteBuffer.wrap(k));
                Optional<V> updatedValue = f.apply(currentValue);
                if (currentValue.equals(updatedValue)) {
                    unlock(key, l);
                    return new CompletedJavaFuture<>(() -> currentValue);
                } else {
                    return new MappedJavaFuture(updatedValue.isPresent() ? put(k, updatedValue.get()) : delete(k)) {
                        @Override
                        public Optional<V> map(Object position) {
                            unlock(key, l);
                            push(key, updatedValue);
                            return updatedValue;
                        }

                        @Override
                        public Boolean recover(Throwable e) throws Throwable {
                            unlock(key, l);
                            throw e;
                        }
                    };
                }
            } catch (Throwable e) {
                unlock(key, l);
                throw e;
            }
        } catch (Throwable e) {
            return new CompletedJavaFuture<>(() -> {
                throw new RuntimeException(e);
            });
        }

    }

    /**
     * An asynchronous non-blocking put operation which inserts or updates the value
     * at the given key. The value is first updated in the kvstore and then a future is created
     * for reflecting the modification in the underlying storage. If the storage write fails
     * the previous value is rolled back in the kvstore and the failure is propagated into
     * the result future.
     *
     * @param key   serialized key
     * @param value new value for the key
     * @return future of the checkpoint that will represent the consistency information after the operation completes
     */
    private Future<?> put(byte[] key, V value) {
        if (readonly) throw new IllegalStateException("put() called on a read-only state");
        long nowMs = System.currentTimeMillis();
        long recordTimestamp = (value instanceof EventTime) ? ((EventTime) value).eventTimeUnix() : nowMs;
        if (ttlMs > 0 && recordTimestamp + ttlMs < nowMs) {
            return delete(key);
        } else {
            byte[] valueBytes = valueSerde.toBytes(value);
            return logOption.map((log) -> log.append(kvstore, key, valueBytes, recordTimestamp)).orElseGet(() -> {
                kvstore.put(ByteBuffer.wrap(key), kvstore.wrap(valueBytes, recordTimestamp));
                return new CompletedJavaFuture<>(null);
            });
        }
    }

    /**
     * An asynchronous non-blocking removal operation which deletes the value
     * at the given key. The value is first removed from the kvstore and then a future is created
     * for reflecting the modification in the underlying storage. If the storage write fails
     * the previous value is rolled back in the kvstore and the failure is propagated into
     * the result future.
     *
     * @param key serialized key to delete
     * @return future of the checkpoint that will represent the consistency information after the operation completes
     */
    private Future<?> delete(byte[] key) {
        if (readonly) throw new IllegalStateException("delete() called on a read-only state");
        return logOption.map((log) -> log.delete(kvstore, key)).orElseGet(() -> {
            kvstore.remove(ByteBuffer.wrap(key));
            return new CompletedJavaFuture<>(null);
        });
    }


  /*
   * Observable State Support
   */

    @Override
    public void internalPush(byte[] key, Optional<byte[]> value) {
        if (value.isPresent()) {
            push(keySerde.fromBytes(key), Optional.of(valueSerde.fromBytes(value.get())));
        } else {
            push(keySerde.fromBytes(key), Optional.empty());
        }
    }

    /**
     * row locking functionality
     */

    private ConcurrentHashMap<K, Long> locks = new ConcurrentHashMap<>();

    private void unlock(K key, Long l) {
        if (!locks.remove(key, l)) {
            throw new IllegalMonitorStateException(key + " is locked by another Thread");
        }
    }

    private Long lock(K key) throws TimeoutException, InterruptedException {
        long l = Thread.currentThread().getId();
        int counter = 0;
        long start = System.currentTimeMillis();
        while (locks.putIfAbsent(key, l) != null) {
            counter += 1;
            long sleepTime = Math.round(Math.log(counter));
            if (sleepTime > 0) {
                if (System.currentTimeMillis() - start > lockTimeoutMs) {
                    throw new TimeoutException("Could not acquire lock for " + key + " in " + lockTimeoutMs + " ms");
                } else {
                    Thread.sleep(sleepTime);
                }
            }
        }
        return l;
    }
}
