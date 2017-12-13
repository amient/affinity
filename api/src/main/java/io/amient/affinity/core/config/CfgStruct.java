package io.amient.affinity.core.config;

import com.typesafe.config.Config;

import java.util.LinkedHashMap;
import java.util.Map;

public class CfgStruct<T extends CfgStruct> extends Cfg<T> {

    private Map<String, Cfg<?>> properties = new LinkedHashMap<>();

    @Override
    void setPath(String path) {
        super.setPath(path);
        properties.forEach((prop, cfg) -> cfg.setPath(extend(prop)));
    }

    @Override
    public T apply(Config config) throws IllegalArgumentException {
        Config c = path().isEmpty() ? config : listPos > -1
                ? config.getConfigList(relPath).get(listPos) : config.getConfig(relPath);
        final StringBuilder errors = new StringBuilder();
        properties.forEach((propPath, cfg) -> {
            try {
                if (c.hasPath(propPath)) {
                    cfg.apply(c);
                } else if (cfg.required) {
                    throw new IllegalArgumentException(propPath + " is required" + (path().isEmpty() ? "" : " in " + path()));
                }
            } catch (IllegalArgumentException e) {
                errors.append(e.getMessage() + "\n");
            }
        });
        c.entrySet().forEach(entry -> {
            if (properties.entrySet().stream().filter( (p) ->
                p.getKey().equals(entry.getKey())
                        || (p.getValue() instanceof CfgGroup<?> && entry.getKey().startsWith(p.getKey()))
            ).count() == 0) {
                errors.append(entry.getKey() + " is not a known property" + (path().isEmpty() ? "" : " of " + path()));
            }
        });
        String errorMessage = errors.toString();
        if (!errorMessage.isEmpty()) throw new IllegalArgumentException(errorMessage);
        return setValue((T)this);
    }

    public CfgString string(String path, boolean required) {
        return add(path, new CfgString(), required);
    }

    public CfgLong longint(String path, boolean required) {
        return add(path, new CfgLong(), required);
    }

    public CfgBool bool(String path, boolean required) {
        return add(path, new CfgBool(), required);
    }

    public CfgInt integer(String path, boolean required) {
        return add(path, new CfgInt(), required);
    }

    public <X extends CfgStruct<X>> X struct(String path, X obj, boolean required) {
        return add(path, obj, required);
    }

    public <X> CfgCls<X> cls(String path, Class<X> c, boolean required) {
        return add(path, new CfgCls<>(c), required);
    }

    public <X extends Cfg<?>> CfgGroup<X> group(String path, Class<X> c, boolean required) {
        return add(path, new CfgGroup<>(c), required);
    }

    public <X, Y extends Cfg<X>> CfgList<X, Y> list(String path, Class<X> c, boolean required) {
        return add(path, new CfgList(c), required);
    }

    private <X extends Cfg<?>> X add(String path, X cfg, boolean required) {
        cfg.setRelPath(path);
        cfg.setPath(path);
        if (!required) cfg.setOptional();
        properties.put(path, cfg);
        return cfg;
    }

}
