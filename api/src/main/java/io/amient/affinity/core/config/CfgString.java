package io.amient.affinity.core.config;

import com.typesafe.config.Config;

public class CfgString extends Cfg<String> {

    @Override
    public String apply(Config config) throws IllegalArgumentException {
        return setValue(listPos > -1 ? config.getStringList(relPath).get(listPos) : config.getString(relPath));
    }
}
