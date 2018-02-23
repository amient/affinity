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

package io.amient.affinity.core.util;

import java.io.Serializable;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Java Optional is not serializable so using this
 * @param <T> option value type
 */
public class JOption<T> implements Serializable {
    private static final JOption<?> EMPTY = new JOption<>();

    private final T value;

    public static<T> JOption<T> empty() {
        @SuppressWarnings("unchecked")
        JOption<T> t = (JOption<T>) EMPTY;
        return t;
    }

    private JOption() {
        this.value = null;
    }

    private JOption(T value) {
        this.value = value;
    }

    public static <T> JOption<T> of(T value) {
        return new JOption<>(value);
    }

    public boolean isPresent() {
        return value != null;
    }

    public T get() {
        if (value == null) {
            throw new NoSuchElementException("No value present");
        }
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof JOption)) {
            return false;
        }

        JOption<?> other = (JOption<?>) obj;
        return Objects.equals(value, other.value);
    }
}
