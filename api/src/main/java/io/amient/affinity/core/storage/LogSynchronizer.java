package io.amient.affinity.core.storage;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public class LogSynchronizer<POS extends Comparable<POS>> extends Thread implements Closeable {

    final private LogStorage<POS> storage;
    final private MemStore kvstore;

    public LogSynchronizer(MemStore kvstore, LogStorage<POS> storage) {
        this.kvstore = kvstore;
        this.storage = storage;
        this.storage.unboundedIterator();
    }

    @Override
    public void run() {
        try {
            while (!interrupted()) {
                Iterator<LogEntry<POS>> x = storage.fetch(true);
                if (x == null) {
                    throw new IllegalStateException("unbounded fetch operation should block instead of returning null");
                }
                //TODO #115 update kvstore
                //              BinaryRecordAndOffset r = records.next();
                //              //for tailing state it means either it is a) replica b) external
                //              if (r.value == null) {
                // TODO #115 updated checkpointer
                //                memstore.unload(r.key, r.offset);
                //                if (tailing) state.internalPush(r.key, Optional.empty());
                //              } else {
                //                memstore.load(r.key, r.value, r.offset, r.timestamp);
                // TODO #115 internalPush
                //                if (tailing) state.internalPush(r.key, Optional.of(r.value));
                //              }
            }
        } catch (InterruptedException e) {
            interrupted();
        } catch (Throwable e) {
            e.printStackTrace();
            // TODO #116 synchronizer thread error propagation
            //            synchronized(Storage.this) {
            //              consumerError.set(e);
            //            }
        }
    }

    @Override
    public void close() throws IOException {
        this.interrupt();
        storage.cancel();
    }
}
