package io.amient.affinity.core.config;

import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigObject;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;

public class ConfigGroup<T, I extends Cfg<T>> extends Cfg<List<T>> {
    private final Class<I> itemClass;
    private Map<String, Cfg<T>> items = new LinkedHashMap<>();

    public ConfigGroup(String path, Class<I> itemClass, Cfg<?> parent) {
        super(parent, path);
        this.itemClass = itemClass;
    }

    public String path(String entry) {
        return super.path()+ "." + entry;
    }


    @Override
    public List<String> validate() {
        List<String> errors = super.validate();
        try {
            ConfigObject group = config.getObject(path);
            group.keySet().forEach((k)-> {
                try {
                    I item = itemClass.newInstance();
                    item.parent = this;
                    item.apply(group.toConfig().getConfig(k));
                    errors.addAll(item.validate());
                    items.put(k, item);
                } catch (InstantiationException e) {
                    errors.add("Cannot Instantiate " + itemClass.getSimpleName());
                } catch (IllegalAccessException e) {
                    errors.add("Cannot Access " + itemClass.getSimpleName());
                }
            });
        } catch (ConfigException.WrongType e) {
            errors.add(e.getMessage());
        }
        return errors;
    }

    @Override
    public List<T> get() {
        return items.values().stream().map(item -> item.get()).collect(Collectors.toList());
    }

    public I apply(String entry) {
        items.forEach((k,v) -> System.err.println(k + " -> " + v));
        if (!items.containsKey(entry)) throw new NoSuchElementException(entry  + " is not configured under " + path());
        return (I)items.get(entry).get();
    }

}
