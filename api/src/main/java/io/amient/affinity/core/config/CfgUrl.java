package io.amient.affinity.core.config;

import com.typesafe.config.Config;

import java.net.MalformedURLException;
import java.net.URL;

public class CfgUrl extends Cfg<URL> {

    @Override
    public CfgUrl apply(Config config) {
        String urlString = listPos > -1 ? config.getStringList(relPath).get(listPos) : config.getString(relPath);
        try {
            return setValue(new URL(urlString));
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e);
        }
    }

}
