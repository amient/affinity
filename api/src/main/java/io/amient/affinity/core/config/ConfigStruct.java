package io.amient.affinity.core.config;

import com.typesafe.config.Config;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ConfigStruct<T extends ConfigStruct> extends Cfg<T> {

    private Map<Cfg<?>, Boolean> properties = new LinkedHashMap<>(); //property -> optional
    public ConfigStruct(String path) {
        super(path);
    }

    protected <C extends Cfg<?>> C required(C cfg) {
        properties.put(cfg, false);
        return cfg;
    }

    protected <C extends Cfg<?>> C optional(C cfg) {
        properties.put(cfg, true);
        return cfg;
    }

    @Override
    public void apply(Config config) {
        super.apply(config);
        properties.forEach((cfg, isOptional) -> {
            cfg.apply(config);
        });
    }

    @Override
    public List<String> validate() throws IllegalArgumentException {
        List<String> errors = super.validate();
        properties.forEach((cfg, isOptional) -> {
            if (!isOptional && !config.hasPath(cfg.path)) {
                errors.add(cfg.path + " is required in " + path);
            } else {
                errors.addAll(cfg.validate());
            }
        });
        return errors;
    }

    @Override
    public T get() {
        return (T)this;
    }

}
