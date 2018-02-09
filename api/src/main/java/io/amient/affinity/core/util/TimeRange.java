package io.amient.affinity.core.util;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.Period;

public class TimeRange implements Serializable {

    static final long serialVersionUID = 1L;

    final public long start;
    final public long end;
    final public long duration;

    public static final TimeRange ALLTIME = new TimeRange(0, Long.MAX_VALUE);

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof TimeRange)) {
            return false;
        } else {
            TimeRange o = (TimeRange) other;
            return (o.start == start && o.end == end);
        }
    }

    public Duration getDuration() {
        return Duration.ofMillis(duration);
    }

    public OffsetDateTime getLocalStart() {
        return EventTime.local(start);
    }

    public OffsetDateTime getLocalEnd() {
        return EventTime.local(end);
    }

    public static TimeRange prev(Duration legnth) {
        long now = EventTime.unix();
        return new TimeRange(now - legnth.toMillis(),now);
    }

    public static TimeRange prev(Period legnth) {
        Instant now = Instant.ofEpochMilli(EventTime.unix());
        return new TimeRange(Instant.from(legnth.subtractFrom(now)).toEpochMilli(), now.toEpochMilli());
    }

    public static TimeRange next(Duration legnth) {
        long now = EventTime.unix();
        return new TimeRange(now, now + legnth.toMillis());
    }

    public static TimeRange next(Period legnth) {
        Instant now = Instant.ofEpochMilli(EventTime.unix());
        return new TimeRange(now.toEpochMilli(), Instant.from(legnth.addTo(now)).toEpochMilli());
    }

    public TimeRange(OffsetDateTime start, OffsetDateTime end) {
        this(start.toInstant(), end.toInstant());
    }

    public TimeRange(Instant start, Instant end) {
        this(start.toEpochMilli(), end.toEpochMilli());
    }

    public TimeRange(Duration until, Instant end) {
        this(end.toEpochMilli() - until.toMillis(), end.toEpochMilli());
    }

    public TimeRange(Instant start, Duration length) {
        this(start.toEpochMilli(), start.toEpochMilli() + length.toMillis());
    }

    public TimeRange(Period until, Instant end) {
        this(Instant.from(until.subtractFrom(end)).toEpochMilli(), end.toEpochMilli());
    }

    public TimeRange(long startEpochMs, long endEpochMs) {
        this.start = startEpochMs;
        this.end = endEpochMs;
        this.duration = endEpochMs - startEpochMs;
    }

}
