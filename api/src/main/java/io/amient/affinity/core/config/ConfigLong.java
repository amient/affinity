package io.amient.affinity.core.config;

import java.util.List;

public class ConfigLong extends Cfg<Long> {

    public ConfigLong(String name) {
        super(name);
    }

    @Override
    public List<String> validate() throws IllegalArgumentException {
        List<String> errors = super.validate();
        if (config.hasPath(path) && !(config.getAnyRef(path) instanceof Long)) errors.add(path + " must be of type Long");
        return errors;
    }

    @Override
    public Long get() {
        return config.getLong(path);
    }
}
