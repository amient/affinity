package io.amient.affinity.core.config;

import com.typesafe.config.Config;

public class CfgInt extends Cfg<Integer> {

    @Override
    public Integer apply(Config config) throws IllegalArgumentException {
        return setValue(config.getInt(relPath));
    }
}
