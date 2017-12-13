package io.amient.affinity.core.config;

import com.typesafe.config.Config;

import java.util.HashMap;
import java.util.Map;

public class CfgGroup<G extends Cfg<?>> extends Cfg<Map<String, G>> {

    private final Class<G> cls;

    public CfgGroup(Class<G> cls) {
        this.cls = cls;
    }

    @Override
    public Map<String, G> apply(Config config) throws IllegalArgumentException {
        Map<String, G> map = new HashMap<>();
        config.getObject(relPath).keySet().forEach((key) -> {
            try {
                G item = cls.newInstance();
                item.setRelPath(key);
                item.setPath(extend(key));
                item.apply(config.getConfig(relPath));
                map.put(key, item);
            } catch (InstantiationException e) {
                throw new IllegalArgumentException(e);
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException(e);
            }

        });
        return setValue(map);
    }

    public G apply(String entry) {
        return value.get(entry);
    }

    public String path(String entry) {
        return extend(entry);
    }
}
