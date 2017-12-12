package io.amient.affinity.core.config;

import com.typesafe.config.Config;

import java.util.LinkedList;
import java.util.List;

public abstract class Cfg<T> {

    final public String path;
    protected Config config = null;

    public Cfg(String path) {
        this.path = path;
    }

    public void apply(Config config) {
        this.config = config;
    }

    protected List<String> validate() {
        if (config == null) throw new IllegalStateException();
        return new LinkedList<>();
    }

    abstract public T get();

    static public <C extends Cfg<?>> C apply(Config config, C cfg) {
        cfg.apply(config);

        List<String> errors = cfg.validate();
        if (errors.size() > 0) throw new IllegalArgumentException(
                errors.stream().reduce("", (i,s) -> s + "\n" + i));
        return cfg;
    }

}