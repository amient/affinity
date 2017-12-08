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
        return Instant.ofEpochMilli(eventTimeUtc()).atZone(ZoneId.systemDefault()).toLocalDateTime();
    }
}
