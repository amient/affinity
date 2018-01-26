package io.amient.affinity.core.storage;

import java.util.*;

public abstract class ObservableState<K> extends Observable {

    class ObservableKeyValue extends Observable {
        @Override
        public void notifyObservers(Object arg) {
            //TODO with atomic cell versioning we could cancel out redundant updates
            setChanged();
            super.notifyObservers(arg);
        }
    }

    /**
     * Observables are attached to individual keys in this State
     */
    private Map<K, ObservableKeyValue> observables = new HashMap<>();

    private ObservableKeyValue getOrCreate(K key) {
        ObservableKeyValue observable = observables.get(key);
        if (observable == null) {
            observable = new ObservableKeyValue();
            observables.put(key, observable);
        }
        return observable;
    }

    public ObservableKeyValue addKeyValueObserver(K key, Observer observer) {
        ObservableKeyValue observable = getOrCreate(key);
        observable.addObserver(observer);
        return observable;
    }

    public Observer addKeyValueObserver(K key, Object initEvent, Observer observer) {
        ObservableKeyValue observable = getOrCreate(key);
        observable.addObserver(observer);
        observer.update(observable, initEvent);
        return observer;
    }

    public void removeKeyValueObserver(K key, Observer observer) {
        ObservableKeyValue observable = observables.get(key);
        if (observable != null) {
            observable.deleteObserver(observer);
            if (observable.countObservers() == 0) observables.remove(key);
        }
    }

    public void push(K key, Object event) {
        try {
            ObservableKeyValue observable = observables.get(key);
            if (observable != null) {
                observable.notifyObservers(event);
            }
        } finally {
            setChanged();
            notifyObservers(new AbstractMap.SimpleEntry(key, event));
        }
    }

    public abstract void internalPush(byte[] key, Optional<byte[]> value);

}
