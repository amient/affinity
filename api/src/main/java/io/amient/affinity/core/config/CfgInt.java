package io.amient.affinity.core.config;

import com.typesafe.config.Config;

public class CfgInt extends Cfg<Integer> {

    @Override
    public CfgInt apply(Config config) throws IllegalArgumentException {
        return setValue(listPos > -1 ? config.getIntList(relPath).get(listPos) : config.getInt(relPath));
    }
}
