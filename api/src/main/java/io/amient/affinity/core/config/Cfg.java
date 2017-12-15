package io.amient.affinity.core.config;

import com.typesafe.config.Config;

import java.util.Optional;

abstract public class Cfg<T> {

    public enum Options {
        STRICT, IGNORE_UNKNOWN
    }

    private String path;

    protected String relPath;

    private Optional<T> value = Optional.empty();
    private Optional<T> defaultValue = Optional.empty();

    protected boolean required = true;
    protected int listPos = -1;

    abstract public Cfg<T> apply(Config config) throws IllegalArgumentException;

    final public<C extends Cfg<T>> C setValue(T value) {
        this.value = Optional.of(value);
        return (C)this;
    }

    final public T apply() {
        return value.isPresent() ? value.get() : defaultValue.get();
    }

    final public String path() {
        return path == null ? "" : path;
    }

    final public String path(String relativePathToRsolve) { return (path == null ? "" : path + ".") + relativePathToRsolve; }

    final public boolean isDefined() {
        return value.isPresent() || defaultValue.isPresent();
    }

    void setOptional() {
        this.required = false;
    }

    void setPath(String path) {
        this.path = path;
    }

    void setListPos(int listPos) { this.listPos = listPos; }

    final void setRelPath(String relPath) {
        this.relPath = relPath;
    }

    final void setDefaultValue(T value) {
        defaultValue = Optional.of(value);
    }



}
