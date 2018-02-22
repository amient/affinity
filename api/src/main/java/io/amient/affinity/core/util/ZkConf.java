package io.amient.affinity.core.util;

import io.amient.affinity.core.config.CfgInt;
import io.amient.affinity.core.config.CfgString;
import io.amient.affinity.core.config.CfgStruct;

public class ZkConf extends CfgStruct<ZkConf> {

    public final CfgString Connect = string("connect", true);
    public final CfgString Root = string("root", true);
    public final CfgInt ConnectTimeoutMs = integer("timeout.connect.ms", true);
    public final CfgInt SessionTimeoutMs = integer("timeout.session.ms", true);

}
