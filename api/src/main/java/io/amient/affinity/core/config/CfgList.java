package io.amient.affinity.core.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class CfgList<L, C extends Cfg<L>> extends Cfg<List<L>> {

    private final Class<C> cls;

    public CfgList(Class<C> cls) {
        this.cls = cls;
    }

    @Override
    public List<L> apply(Config config) throws IllegalArgumentException {
        List<L> list = new LinkedList<>();
        Iterator<ConfigValue> it = config.getList(relPath).iterator();
        int i = 0;
        while(it.hasNext()) {
            try {
                ConfigValue el = it.next();
                C item = cls.newInstance();
                item.setListPos(i);
                item.setRelPath(relPath);
                item.apply(config);
                list.add((L)el.unwrapped());
            } catch (InstantiationException e) {
                throw new IllegalArgumentException(e);
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException(e);
            }

        }
        return setValue(list);
    }

    public L apply(Integer entry) {
        return value.get(entry);
    }

    public String path(String entry) {
        return extend(entry);
    }
}
