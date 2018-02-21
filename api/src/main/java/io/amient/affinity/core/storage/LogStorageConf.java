package io.amient.affinity.core.storage;

import io.amient.affinity.core.config.CfgCls;
import io.amient.affinity.core.config.CfgLong;
import io.amient.affinity.core.config.CfgStruct;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class LogStorageConf extends CfgStruct<LogStorageConf> {
    public CfgCls<LogStorage> Class = cls("class", LogStorage.class, false);
    public CfgLong MinTimestamp = longint("min.timestamp.ms", 0L);

    @Override
    protected Set<String> specializations() {
        return new HashSet<>(Arrays.asList("kafka"));
    }

}
