package io.amient.affinity.core.storage;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

public interface EventTime {

    /**
     * event time in milliseconds since the epoch
     * @return
     */
    long eventTimeUtc();

    /**
     * @return event time translated into LocalDate instance
     */
    default LocalDate eventTimeLocal() {
        return Instant.ofEpochMilli(eventTimeUtc()).atZone(ZoneId.systemDefault()).toLocalDate();
    }
}
