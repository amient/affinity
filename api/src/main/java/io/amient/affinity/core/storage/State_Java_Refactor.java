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

    /**
     * This is similar to apply(key) except it also applies row lock which is useful if the client
     * wants to make sure that the returned value incorporates all changes applied to it in update operations
     * with in the same sequence.
     *
     * @param key
     * @return
     */
    Optional<V> get(K key) throws TimeoutException, InterruptedException {
        Long l = lock(key);
        try {
            return apply(key);
        } finally {
            unlock(key, l);
        }
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
     * update is a syntactic sugar for update where the value is always overriden
     *
     * @param key   to updateImpl
     * @param value new value to be associated with the key
     * @return Future Optional of the value previously held at the key position
     */
    public Future<Optional<V>> update(K key, V value) {
//        update(key) {
//            case Some(prev)
//                if prev == value =>(None, Some(prev), Some(prev))
//            case Some(prev) =>(Some(value), Some(value), Some(prev))
//            case None =>(Some(value), Some(value), None)
//        }
        //TODO
        return null;
    }

    /**
     * remove is a is a syntactic sugar for update where None is used as Value
     * it is different from delete in that it returns the removed value
     * which is more costly.
     *
     * @param key     to remove
     * @param command is the message that will be pushed to key-value observers
     * @return Future Optional of the value previously held at the key position
     */
    public Future<Optional<V>>  remove(K key, Object command) {
//        update(key) {
//            case None =>(None, None, None)
//            case Some(component) =>(Some(command), None, Some(component))
//        }
        //TODO
        return null;
    }


    /**
     * insert is a syntactic sugar for putImpl where the value is overriden if it doesn't exist
     * and the command is the value itself
     *
     * @param key   to insert
     * @param value new value to be associated with the key
     * @return Future Optional of the value previously held at the key position
     */
    public Future<V> insert(K key, V value){
//        update(key) {
//            case Some(_) => throw new IllegalArgumentException(s"$key already exists in state store")
//            case None => (Some(value), Some(value), value)
//        }
        //TODO
        return null;
    }

    /**
     * update enables per-key observer pattern for incremental updates.
     *
     * @param <R> result type
     * @param key  key which is going to be updated
     * @param readBeforeWrite a function which takes an existing value of the key and returns a result type
     * TODO param pf   putImpl function which maps the current value Option[V] at the given key to 3 values:
     *             1. Option[Any] is the incremental putImpl event
     *             2. Option[V] is the new state for the given key as a result of the incremntal putImpl
     *             3. R which is the result value expected by the caller
     * @return Future[R] which will be successful if the put operation of Option[V] of the pf succeeds
     */
    public <R> Future<R> update(K key, Function<Optional<V>, R> readBeforeWrite) {
        //TODO
        return null;
    }
//    def update[R](key: K)(pf: PartialFunction[Option[V], (Option[Any], Option[V], R)]): Future[R] = {
//        try {
//            val k = keySerde.toBytes(key)
//            val l = lock(key)
//            try {
//                pf(apply(ByteBuffer.wrap(k))) match {
//                    case (None, _, result) =>
//                        unlock(key, l)
//                        Future.successful(result)
//                    case (Some(increment), changed, result) => changed match {
//                        case Some(updatedValue) =>
//                            put(k, updatedValue) transform( {
//                                s => unlock(key, l); s
//              }, {
//                            e => unlock(key, l); e
//                        }) andThen {
//                            case _ => push(key, increment)
//                        } map (_ => result)
//                        case None =>
//                            delete(k) transform( {
//                                s => unlock(key, l); s
//              }, {
//                            e => unlock(key, l); e
//                        }) andThen {
//                            case _ => push(key, increment)
//                        } map (_ => result)
//                    }
//                }
//            } catch {
//                case e: Throwable =>
//                    unlock(key, l)
//                    throw e
//            }
//        } catch {
//            case NonFatal(e) => Future.failed(e)
//        }
//    }


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
