package io.amient.affinity.core.state;

import java.util.Observable;

class ObservableKeyValue extends Observable {
    @Override
    public void notifyObservers(Object arg) {
        setChanged();
        super.notifyObservers(arg);
    }
}
