package io.amient.affinity.core.config;

import java.util.List;

public class ConfigInt extends Cfg<Integer> {

    public ConfigInt(String name) {
        super(name);
    }

    @Override
    public List<String> validate() throws IllegalArgumentException {
        List<String> errors = super.validate();
        if (config.hasPath(path) && !(config.getAnyRef(path) instanceof Integer)) errors.add(path + " must be of type Integer");
        return errors;
    }


    @Override
    public Integer get() {
        return config.getInt(path);
    }
}
