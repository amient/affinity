package io.amient.affinity.core.state;

import java.util.Observer;

public interface ObservableKVStore<K> {
    ObservableKeyValue addKeyValueObserver(K key, Observer observer);

    Observer addKeyValueObserver(K key, Object initEvent, Observer observer);

    void removeKeyValueObserver(K key, Observer observer);
}
