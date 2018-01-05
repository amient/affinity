package io.amient.affinity.core.storage;

import io.amient.affinity.core.config.CfgInt;
import io.amient.affinity.core.config.CfgLong;
import io.amient.affinity.core.config.CfgString;
import io.amient.affinity.core.config.CfgStruct;
import io.amient.affinity.core.storage.Storage.StorageConf;
import io.amient.affinity.core.storage.MemStore.MemStoreConf;

public class StateConf extends CfgStruct<StateConf> {
    public CfgInt TtlSeconds = integer("ttl.sec", -1);
    public CfgLong MinTimestamp = longint("min.timestamp", 0L);
    public StorageConf Storage = struct("storage", new StorageConf(), false);
    public MemStoreConf MemStore = struct("memstore", new MemStore.MemStoreConf(), true);
    public CfgInt LockTimeoutMs = integer("lock.timeout.ms", 10000);
}

