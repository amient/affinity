package io.amient.affinity.core.config;

import com.typesafe.config.Config;

public class CfgBool extends Cfg<Boolean> {

    @Override
    public Boolean apply(Config config) throws IllegalArgumentException {
        return setValue(config.getBoolean(relPath));
    }
}
