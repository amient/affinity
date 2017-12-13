package io.amient.affinity.core.config;

import com.typesafe.config.Config;

public class CfgBool extends Cfg<Boolean> {

    @Override
    public CfgBool apply(Config config) throws IllegalArgumentException {
        return setValue(listPos > -1 ? config.getBooleanList(relPath).get(listPos) : config.getBoolean(relPath));
    }
}
