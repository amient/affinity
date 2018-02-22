package io.amient.affinity.core.util;

import io.amient.affinity.core.config.CfgInt;
import io.amient.affinity.core.config.CfgString;
import io.amient.affinity.core.config.CfgStruct;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class ZkConf extends CfgStruct<ZkConf> {

    public final CfgString Connect = string("connect", true);
    public final CfgInt ConnectTimeoutMs = integer("timeout.connect.ms", 6000);
    public final CfgInt SessionTimeoutMs = integer("timeout.session.ms", 10000);

    @Override
    protected Set<String> specializations() {
        return new HashSet<>(Arrays.asList("root"));
    }
}
