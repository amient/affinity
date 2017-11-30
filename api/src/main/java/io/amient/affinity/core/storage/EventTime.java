package io.amient.affinity.core.storage;

public interface EventTime {

    /**
     * event time in milliseconds since the epoch
     * @return
     */
    long eventTimeMs();
}
