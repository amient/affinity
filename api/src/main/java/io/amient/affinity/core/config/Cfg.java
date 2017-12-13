package io.amient.affinity.core.config;

import com.typesafe.config.Config;

abstract public class Cfg<T> {

    private String path;

    protected String relPath;

    protected T value;

    protected boolean required = true;

    abstract public T apply(Config config) throws IllegalArgumentException;

    protected String extend(String thatPath) {
        return (path == null ? "" : path + ".") + thatPath;
    }
    final protected T setValue(T value) {
        this.value = value;
        return value;
    }
    final public T apply() {
        return value;
    }
    void setOptional() {
        this.required = false;
    }
    void setPath(String path) {
        this.path = path;
    }
    
    final void setRelPath(String relPath) {
        this.relPath = relPath;
    }

    final String path() {
        return this.path == null ? "" : this.path;
    }
}
