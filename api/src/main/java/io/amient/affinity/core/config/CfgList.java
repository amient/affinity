package io.amient.affinity.core.config;

import com.typesafe.config.Config;

import java.util.LinkedList;
import java.util.List;

public class CfgList<L extends Cfg<?>> extends Cfg<List<L>> {

    private final Class<L> cls;

    public CfgList(Class<L> cls) {
        this.cls = cls;
    }

    @Override
    public List<L> apply(Config config) throws IllegalArgumentException {
        List<L> list = new LinkedList<>();
        config.getList(relPath).forEach((itemValue) -> {
            try {
                L item = cls.newInstance();
                //item.setRelPath(i);
                item.apply(config.getConfig(relPath));
                list.add(item);
            } catch (InstantiationException e) {
                throw new IllegalArgumentException(e);
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException(e);
            }

        });
        return setValue(list);
    }

    public L apply(Integer entry) {
        return value.get(entry);
    }

    public String path(String entry) {
        return extend(entry);
    }
}
