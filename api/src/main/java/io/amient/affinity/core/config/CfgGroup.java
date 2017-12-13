package io.amient.affinity.core.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;

import java.util.HashMap;
import java.util.Map;

public class CfgGroup<G extends Cfg<?>> extends Cfg<Map<String, G>> implements CfgNested {

    private final Class<G> cls;

    public CfgGroup(Class<G> cls) {
        this.cls = cls;
    }

    @Override
    public CfgGroup<G> apply(Config config) throws IllegalArgumentException {
        ConfigObject o = listPos > -1 ? config.getObjectList(relPath).get(listPos) : config.getObject(relPath);
        Map<String, G> map = new HashMap<>();
        o.keySet().forEach((key) -> {
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
