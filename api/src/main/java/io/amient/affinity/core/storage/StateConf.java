package io.amient.affinity.core.storage;

import io.amient.affinity.core.config.*;
import io.amient.affinity.core.storage.MemStore.MemStoreConf;

public class StateConf extends CfgStruct<StateConf> {
    public CfgInt TtlSeconds = integer("ttl.sec", -1);
    public CfgBool External = bool("external", true, false);
    public CfgLong MinTimestampUnixMs = longint("min.timestamp.ms", 0L);
    public io.amient.affinity.core.storage.Storage.StorageConf Storage = struct("storage", new Storage.StorageConf(), false);
    public MemStoreConf MemStore = struct("memstore", new MemStore.MemStoreConf(), true);
    public CfgInt LockTimeoutMs = integer("lock.timeout.ms", 10000);
}

