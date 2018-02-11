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
