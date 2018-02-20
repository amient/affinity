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

public class Log<POS extends Comparable<POS>> extends Thread implements Closeable {

    private final static Logger log = LoggerFactory.getLogger(Log.class);

    final private AtomicReference<POS> checkpoint = new AtomicReference<>(null);

    volatile private boolean checkpointModified = false;

    volatile private boolean stopped = true;

    final private boolean enabled;
    final private Path checkpointFile;

    final private LogStorage<POS> storage;

    private AtomicReference<LogSynchronizer<POS>> synchronizer = new AtomicReference<>();

    public Log(LogStorage<POS> storage, Path checkpointFile) {
        this.storage = storage;
        this.enabled = checkpointFile != null;
        this.checkpointFile = checkpointFile;
        if (enabled) {
            if (Files.exists(checkpointFile)) try {
                ObjectInputStream ois = new ObjectInputStream(new FileInputStream(checkpointFile.toFile()));
                try {
                    checkpoint.set((POS) ois.readObject());
                } finally {
                    ois.close();
                }
            } catch (Throwable e) {
                log.warn("Invalid checkpoint file: " + checkpointFile + ", going to rewind fully.", e);
                checkpoint.set(null);
            }
            log.info("Initialized " + checkpoint + " from " + checkpointFile);
        }
    }

    public POS getCheckpoint() {
        return checkpoint.get();
    }

    public Future<POS> append(final MemStore kvstore, final byte[] key, byte[] valueBytes, final long recordTimestamp) {
        Record record = new Record(key, valueBytes, recordTimestamp);
        return new MappedJavaFuture<POS, POS>(storage.append(record)) {
            @Override
            public POS map(POS position) {
                kvstore.put(ByteBuffer.wrap(key), kvstore.wrap(valueBytes, recordTimestamp));
                updateCheckpoint(position);
                return position;
            }
        };
    }

    public Future<POS> delete(final MemStore kvstore, final byte[] key) {
        return new MappedJavaFuture<POS, POS>(storage.delete(key)) {
            @Override
            public POS map(POS position) {
                kvstore.remove(ByteBuffer.wrap(key));
                updateCheckpoint(position);
                return position;
            }
        };
    }

    public void bootstrap(final MemStore kvstore, int partition) throws IOException {
        stopSynchronizerIfRunning();
        POS checkpoint = getCheckpoint();
        if (checkpoint != null) storage.reset(partition, checkpoint);

        Iterator<LogEntry<POS>> i = storage.boundedIterator();
        while (i.hasNext()) {
            LogEntry<POS> entry = i.next();
            kvstore.put(ByteBuffer.wrap(entry.key), kvstore.wrap(entry.value, entry.timestamp));
            updateCheckpoint(entry.position);
        }
    }

    public void tail(final MemStore kvstore, int partition) {
        synchronizer.set(new LogSynchronizer(kvstore, storage));
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
        try {
            stopSynchronizerIfRunning();
            if (enabled) writeCheckpoint();
        } finally {
            stopped = true;
        }
    }

    private void stopSynchronizerIfRunning() throws IOException {
        LogSynchronizer<POS> sync = synchronizer.get();
        if (sync != null) {
            synchronizer.compareAndSet(sync, null);
            sync.close();
        }
    }

    private POS updateCheckpoint(POS position) {
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
        POS position = checkpoint.get();
        log.debug("Writing checkpoint " + position + " to file: " + checkpointFile);
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(checkpointFile.toFile()));
        oos.writeObject(position);
        oos.close();
        checkpointModified = false;
    }

}


