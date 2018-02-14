package io.amient.affinity.core.util;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class ThreadLocalCache<K, V> extends ThreadLocal<HashMap<K, V>> {

    ConcurrentHashMap<K, V> global = new ConcurrentHashMap<>();

    @Override
    protected HashMap<K, V> initialValue() {
        return new HashMap<>();
    }

    public void initialize(K key, V value) {
        global.put(key, value);
    }

    public V getOrInitialize(K key, Supplier<V> supplier) {
        HashMap<K, V> instance = get();
        V current = instance.get(key);
        if (current != null) return current;
        V initialized = global.get(key);
        if (initialized == null) {
            initialized = supplier.get();
            global.put(key, initialized);
        }
        instance.put(key, initialized);
        return initialized;
    }

}
