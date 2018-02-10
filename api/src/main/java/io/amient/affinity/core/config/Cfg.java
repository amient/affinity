package io.amient.affinity.core.config;

import com.typesafe.config.Config;
import io.amient.affinity.core.util.JOption;

import java.io.Serializable;
import java.util.NoSuchElementException;

abstract public class Cfg<T> implements Serializable {

    public enum Options {
        STRICT, IGNORE_UNKNOWN
    }

    private String path;

    protected String relPath;

    private JOption<T> value = JOption.empty();
    private JOption<T> defaultValue = JOption.empty();

    protected boolean required = true;
    protected int listPos = -1;

    abstract public Cfg<T> apply(Config config) throws IllegalArgumentException;

    final public <C extends Cfg<T>> C setValue(T value) {
        //TODO using setValue should modify the underlying config object
        this.value = JOption.of(value);
        return (C) this;
    }

    final public T apply() {
        if (value.isPresent()) return value.get();
        else if (defaultValue.isPresent()) return defaultValue.get();
        else throw new NoSuchElementException(path + " is not defined");
    }

    final public String path() {
        return path == null ? "" : path;
    }

    final public String path(String relativePathToRsolve) {
        return (path == null ? "" : path + ".") + relativePathToRsolve;
    }

    final public boolean isDefined() {
        return value.isPresent() || defaultValue.isPresent();
    }

    void setOptional() {
        this.required = false;
    }

    void setPath(String path) {
        this.path = path;
    }

    void setListPos(int listPos) {
        this.listPos = listPos;
    }

    final void setRelPath(String relPath) {
        this.relPath = relPath;
    }

    final void setDefaultValue(T value) {
        defaultValue = JOption.of(value);
    }


}
