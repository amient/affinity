package io.amient.affinity.core.storage;

import io.amient.affinity.core.config.*;
import io.amient.affinity.core.storage.MemStore.MemStoreConf;

public class StateConf extends CfgStruct<StateConf> {
    public CfgInt TtlSeconds = integer("ttl.sec", -1);
    public CfgBool External = bool("external", true, false);
    public CfgLong MinTimestampUnixMs = longint("min.timestamp.ms", 0L);
    public LogStorageConf Storage = struct("storage", new LogStorageConf());
    public MemStoreConf MemStore = struct("memstore", new MemStore.MemStoreConf());
    public CfgInt LockTimeoutMs = integer("lock.timeout.ms", 10000);
}

