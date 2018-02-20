package io.amient.affinity.core.storage;

import io.amient.affinity.core.util.MappedJavaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

public class Log<P extends Comparable<P>> extends Thread implements Closeable {

    private final static Logger log = LoggerFactory.getLogger(Log.class);

    final private AtomicReference<P> checkpoint = new AtomicReference<>(null);

    volatile private boolean checkpointModified = false;

    volatile private boolean stopped = true;

    final private boolean enabled;
    final private Path file;

    //TODO #115 storage should be private
    final public LogStorage<P> storage;

    public Log(LogStorage<P> storage, Path file) {
        this.storage = storage;
        this.enabled = file != null;
        this.file = file;
        if (enabled) {
            if (Files.exists(file)) try {
                ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file.toFile()));
                try {
                    checkpoint.set((P) ois.readObject());
                } finally {
                    ois.close();
                }
            } catch (Throwable e) {
                log.warn("Invalid checkpoint file: " + file + ", going to rewind fully.", e);
                checkpoint.set(null);
            }
            log.info("Initialized " + checkpoint + " from " + file);
        }
    }

    public P getCheckpoint() {
        return checkpoint.get();
    }

    public Future<P> append(final MemStore kvstore, final byte[] key, byte[] valueBytes, final long recordTimestamp) {
        Record record = new Record(key, valueBytes, recordTimestamp);
        return new MappedJavaFuture<P, P>(storage.append(record)) {
            @Override
            public P map(P position) {
                kvstore.put(ByteBuffer.wrap(key), kvstore.wrap(valueBytes, recordTimestamp));
                updateCheckpoint(position);
                return position;
            }
        };
    }

    public Future<P> delete(final MemStore kvstore, final byte[] key) {
        return new MappedJavaFuture<P,P>(storage.delete(key)) {
            @Override
            public P map(P position) {
                kvstore.remove(ByteBuffer.wrap(key));
                updateCheckpoint(position);
                return position;
            }
        };
    }

    public void bootstrap(final MemStore kvstore, int partition) {
        if (getCheckpoint() != null) storage.reset(partition, getCheckpoint());
        Iterator<LogEntry<P>> i = storage.boundedIterator();
        while (i.hasNext()) {
            LogEntry<P> entry = i.next();
            kvstore.put(ByteBuffer.wrap(entry.key), kvstore.wrap(entry.value, entry.timestamp));
            updateCheckpoint(entry.position);
        }
    }

    @Override
    public void run() {
        try {
            stopped = false;
            while (!stopped) {
                Thread.sleep(10000); //TODO make checkpoint interval configurable
                if (enabled && checkpointModified) {
                    writeCheckpoint();
                }
            }
        } catch (Exception e) {
            log.error("Error in the manager thread", e);
            Thread.currentThread().getThreadGroup().interrupt();
        }
    }

    public void close() throws IOException {
        if (enabled) writeCheckpoint();
        stopped = true;
    }

    private P updateCheckpoint(P position) {
        return checkpoint.updateAndGet(chk -> {
            if (log.isTraceEnabled()) {
                log.trace("updating checkpoint, offset: " + position);
            }
            if (chk == null || position.compareTo(chk) > 0) {
                if (enabled) checkpointModified = true;
                return position;
            } else {
                return chk;
            }
        });
    }

    private void writeCheckpoint() throws IOException {
        P position = checkpoint.get();
        log.debug("Writing checkpoint " + position + " to file: " + file);
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file.toFile()));
        oos.writeObject(position);
        oos.close();
        checkpointModified = false;
    }

}


