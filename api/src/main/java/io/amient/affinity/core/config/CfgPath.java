package io.amient.affinity.core.config;

import com.typesafe.config.Config;

import java.nio.file.Path;
import java.nio.file.Paths;

public class CfgPath extends Cfg<Path> {

    @Override
    public CfgPath apply(Config config) {
        String urlString = listPos > -1 ? config.getStringList(relPath).get(listPos) : config.getString(relPath);
        return setValue(Paths.get(urlString).toAbsolutePath());
    }

}
