package io.amient.affinity.core.config;

import com.typesafe.config.Config;

public class CfgLong extends Cfg<Long> {

    @Override
    public Long apply(Config config) throws IllegalArgumentException {
        return setValue(config.getLong(relPath));
    }
}