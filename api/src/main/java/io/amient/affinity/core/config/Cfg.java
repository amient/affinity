package io.amient.affinity.core.config;

import com.typesafe.config.Config;

abstract public class Cfg<T> {

    private String path;

    protected String relPath;

    protected T value;

    protected boolean required = true;
    protected int listPos = -1;

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

    final public String path() {
        return this.path == null ? "" : this.path;
    }

    public void setListPos(int listPos) {
        this.listPos = listPos;
    }
}
