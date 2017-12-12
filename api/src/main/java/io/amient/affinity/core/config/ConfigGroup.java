package io.amient.affinity.core.config;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class ConfigGroup<T, I extends Cfg<T>> extends Cfg<List<T>> {
    private final Class<I> itemClass;
    private List<Cfg<T>> items = new LinkedList<>();

    public ConfigGroup(String path, Class<I> itemClass) {
        super(path);
        this.itemClass = itemClass;
    }

    @Override
    public List<String> validate() throws IllegalArgumentException {
        List<String> errors = super.validate();
        config.getConfigList(path).stream().forEach(itemConfig -> {
            try {
                items.add(itemClass.newInstance());
            } catch (InstantiationException e) {
                errors.add("Cannot Instantiate " + itemClass.getSimpleName());
            } catch (IllegalAccessException e) {
                errors.add("Cannot Access " + itemClass.getSimpleName());
            }
        });
        return errors;
    }

    @Override
    public List<T> get() {
        return items.stream().map(item -> item.get()).collect(Collectors.toList());
    }

}
