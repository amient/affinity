/*
 * Copyright 2016-2018 Michal Harish, michal.harish@gmail.com
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.amient.affinity.core.config;

import com.typesafe.config.Config;
import io.amient.affinity.core.util.JavaOption;

import java.io.Serializable;
import java.util.NoSuchElementException;

abstract public class Cfg<T> implements Serializable {

    public enum Options {
        STRICT, IGNORE_UNKNOWN
    }

    private String path = "";

    protected String relPath;

    private JavaOption<T> value = JavaOption.empty();
    private JavaOption<T> defaultValue = JavaOption.empty();

    protected boolean required = true;
    protected int listPos = -1;

    abstract public Cfg<T> apply(Config config) throws IllegalArgumentException;

    final public <C extends Cfg<T>> C setValue(T value) {
        //TODO using setValue should modify the underlying config object
        this.value = JavaOption.of(value);
        return (C) this;
    }

    final public T apply() {
        if (value.isPresent()) return value.get();
        else if (defaultValue.isPresent()) return defaultValue.get();
        else throw new NoSuchElementException(path + " is not defined");
    }

    final public String path() {
        return path;
    }

    final public String path(String relativePathToRsolve) {
        return (path.isEmpty() ? "" : path + ".") + relativePathToRsolve;
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
        defaultValue = JavaOption.of(value);
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode() + (isDefined() && apply() != this ? apply().hashCode() : -1);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Cfg)) {
            return false;
        } else {
            Cfg that = (Cfg) other;
            if (!this.getClass().equals(that.getClass())) return false;
            if (that.isDefined() && this.isDefined() && apply().equals(that.apply())) {
                return true;
            } else {
                return !that.isDefined() && !this.isDefined();
            }
        }
    }

    @Override
    public String toString() {
        return isDefined() ? apply().toString() : "undefined";
    }


}
