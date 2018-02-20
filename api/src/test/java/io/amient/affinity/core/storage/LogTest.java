package io.amient.affinity.core.storage;

import com.typesafe.config.ConfigFactory;
import io.amient.affinity.core.util.MemoryLogStorage;
import io.amient.affinity.core.util.TimeRange;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

public class LogTest {

    @Test
    public void testLogBootstrapCheckpointAndTail() throws IOException, ExecutionException, InterruptedException {
        StateConf template = new StateConf();
        StateConf stateConf = new StateConf().apply(ConfigFactory.parseMap(new HashMap<String, String>(){{
            put(template.MemStore.Class.path(), MemStoreSimpleMap.class.getName());
        }}));
        MemStore kvstore = new MemStoreSimpleMap(stateConf);
        LogStorage storage = new MemoryLogStorage(stateConf.Storage);
        Future<Long> w1 = storage.append(new Record<>("key1".getBytes(), "value1".getBytes(), 1L));
        Future<Long> w2 = storage.append(new Record<>("key2".getBytes(), "value2".getBytes(), 1L));
        Future<Long> w3 = storage.append(new Record<>("key1".getBytes(), "value10".getBytes(), 2L));
        Future<Long> w4 = storage.append(new Record<>("key2".getBytes(), "value20".getBytes(), 2L));
        storage.flush();
        assert(w1.get() == 0L);
        assert(w2.get() == 1L);
        assert(w3.get() == 2L);
        assert(w4.get() == 3L);
        Path checkpointFile = Files.createTempFile("testmemstore",".checkpoint");
        checkpointFile.toFile().deleteOnExit();

        //test initial bootstrap with no checkpoint
        Log<Long> log = storage.open(checkpointFile);
        try {
            assert (log.getCheckpoint() == null);
            long numRecordsBootstrapped = log.bootstrap(kvstore, 0);
            assert (log.getCheckpoint() == 3);
            assert (numRecordsBootstrapped == 4);
            assert (kvstore.numKeys() == 2);
        } finally {
            log.close();

        }

        //test bootstrap with previous checkpoint and tail
        log = storage.open(checkpointFile);
        try {
            assert(log.getCheckpoint() == 3); //opened from the previous checkpoint
            assert(log.bootstrap(kvstore, 0) == 0); //checkpoint is up-to-date, nothing to bootstrap
            log.tail(kvstore, null);
            storage.append(new Record<>("key3".getBytes(), "value3".getBytes(), 11L));
            storage.append(new Record<>("key3".getBytes(), "value30".getBytes(), 12L));
            storage.append(new Record<>("key3".getBytes(), "value300".getBytes(), 13L));
            storage.flush();
            Thread.sleep(250);
            assert(log.bootstrap(kvstore, 0) == 0);
            assert(kvstore.numKeys() == 3);
        } finally {
            log.close();
        }

        //test a bounded range scan
        storage.reset(new TimeRange(11L, 13L)); //start time is inclusive, end time is exclusive
        Iterator<LogEntry<Long>> rangeScan = storage.boundedIterator();
        assert(rangeScan.hasNext());
        assert(new String(rangeScan.next().value).equals("value3"));
        assert(rangeScan.hasNext());
        assert(new String(rangeScan.next().value).equals("value30"));
        assert(!rangeScan.hasNext());

        storage.close();
    }
}
