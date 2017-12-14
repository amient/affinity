package io.amient.affinity.core.storage;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public interface EventTime {

    /**
     * event time since the epoch
     * @return long milliseconds
     */
    long eventTimeUtc();

    /**
     * @return event time translated into LocalDate instance
     */
    default LocalDateTime eventTimeLocal() {
        return localTime(eventTimeUtc());
    }

    /**
     * @param utc milliseconds since the epoch
     * @return LocalDate instance for the given utc timestamp
     */
    static LocalDateTime localTime(long utc) {
        return Instant.ofEpochMilli(utc).atZone(ZoneId.systemDefault()).toLocalDateTime();
    }
}
