package io.amient.affinity.core.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ConfigStruct<T extends ConfigStruct> extends Cfg<T> {

    private Map<Cfg<?>, Boolean> properties = new LinkedHashMap<>(); //property -> optional

    public ConfigStruct() {
        this(null, "");
    }
    public ConfigStruct(String path) {
        this(null, path);
    }

    public ConfigStruct(Cfg<?> parent, String path) {
        super(parent, path);
    }

    protected <A extends Cfg> A struct(String _path, Class<A> structClass) throws IllegalAccessException, InstantiationException {
        A struct = structClass.newInstance();
        struct.parent = this;
        return struct;
    }

    protected <A, I extends Cfg<A>> ConfigGroup<A, I> group(String _path, Class<I> itemClass) {
        return new ConfigGroup(_path, itemClass, this);
    }

    protected <T> Cfg<Class<? extends T>> cls(String _path, Class<T> bound) {
        return new Cfg<Class<? extends T>>(this, _path) {

            private Class<? extends T> value  = null;

            @Override
            public List<String> validate() {
                List<String> errors = super.validate();
                if (config.hasPath(path)) {
                    String fqn = config.getString(path);
                    try {
                        Class<?> rawClass = Class.forName(fqn);
                        value = rawClass.asSubclass(bound);
                    } catch (ClassCastException e1) {
                        errors.add(fqn + " is not an instance of " + bound.getName());
                    } catch (ClassNotFoundException e) {
                        errors.add("Class not found: " + fqn);
                    }
                }
                return errors;
            }

            @Override
            public Class<? extends T> get() {
                return value;
            }
        };
    }

    protected Cfg<String> str(String _path) {
        return new Cfg<String>(this, _path) {

            @Override
            public List<String> validate() {
                List<String> errors = super.validate();
                if (config.hasPath(path) && !(config.getAnyRef(path) instanceof String))
                    errors.add(path + " must be of type String");
                return errors;
            }

            @Override
            public String get() {
                return config.getString(path);
            }
        };
    }
    protected Cfg<Integer> intProperty(String _path) {
        return new Cfg<Integer>(this, _path) {

            @Override
            public List<String> validate() {
                List<String> errors = super.validate();
                if (config.hasPath(path) && !(config.getAnyRef(path) instanceof Integer))
                    errors.add(path + " must be of type Integer");
                return errors;
            }

            @Override
            public Integer get() {
                return config.getInt(path);
            }
        };
    }

    protected Cfg<Long> longint(String _path) {
        return new Cfg<Long>(this, _path) {

            @Override
            public List<String> validate() {
                List<String> errors = super.validate();
                if (config.hasPath(path) && !(config.getAnyRef(path) instanceof Long))
                    errors.add(path + " must be of type Long");
                return errors;
            }

            @Override
            public Long get() {
                return config.getLong(path);
            }
        };
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
        super.apply(path.isEmpty() ? config : (config.hasPath(path) ? config.getConfig(path) : ConfigFactory.empty() ));
        properties.forEach((cfg, isOptional) -> cfg.apply(this.config));
    }

    @Override
    public List<String> validate() throws IllegalArgumentException {
        List<String> errors = super.validate();
        properties.forEach((cfg, isOptional) -> {
            if (config.hasPath(cfg.path)) {
                errors.addAll(cfg.validate());
            } else if (!isOptional) {
                errors.add(cfg.path + " is required in " + path());
            }
        });
        config.entrySet().forEach(entry -> {
           if (properties.keySet().stream().filter(i ->
                   (entry.getKey().equals(i.path) || entry.getKey().startsWith(i.path))
                ).count() == 0) {
               errors.add(entry.getKey() + " is not a known property of " + path());
           }
        });
        return errors;
    }

    @Override
    public T get() {
        return (T) this;
    }

}
