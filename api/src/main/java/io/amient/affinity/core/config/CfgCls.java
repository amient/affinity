package io.amient.affinity.core.config;

import com.typesafe.config.Config;

public class CfgCls<B> extends Cfg<Class<? extends B>> {

    private final Class<? extends B> cls;

    public CfgCls(Class<? extends B> cls) {
        this.cls = cls;
    }

    @Override
    public Class<? extends B> apply(Config config) {
        String fqn = listPos > -1 ? config.getStringList(relPath).get(listPos) : config.getString(relPath);
        try {
            return setValue(Class.forName(fqn).asSubclass(cls));
        } catch (ClassCastException e) {
            throw new IllegalArgumentException(fqn + " is not an instance of " + cls);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

}
