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

    private static final Long UNBOUNDED_MIN_TIME = 0L;

    private static final Long UNBOUNDED_MAX_TIME = Long.MAX_VALUE;

    public static final TimeRange UNBOUNDED = new TimeRange(UNBOUNDED_MIN_TIME, UNBOUNDED_MAX_TIME);

    @Override
    public String toString() {
        String from = start == UNBOUNDED_MIN_TIME ? "∞" : getLocalStart().toString();
        String to = end == UNBOUNDED_MAX_TIME ? "∞" : getLocalEnd().toString();
        return from + " .. " + to;
    }

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

    public static TimeRange since(OffsetDateTime datetime) {
        return since(datetime.toInstant().toEpochMilli());
    }

    public static TimeRange since(long unixtimestamp) {
        return new TimeRange(unixtimestamp, UNBOUNDED_MAX_TIME);
    }

    public static TimeRange until(OffsetDateTime datetime) {
        return until(datetime.toInstant().toEpochMilli());
    }

    public static TimeRange until(long unixtimestamp) {
        return new TimeRange(0, unixtimestamp);
    }


    public static TimeRange prev(Duration length) {
        return prev(length, Instant.ofEpochMilli(EventTime.unix()));
    }

    public static TimeRange prev(Duration length, String beforeDatetime) {
        return prev(length, EventTime.unix(beforeDatetime));
    }

    public static TimeRange prev(Duration length, Long beforeEpochmillis) {
        return prev(length, Instant.ofEpochMilli(beforeEpochmillis));
    }

    public static TimeRange prev(Duration length, Instant before) {
        return new TimeRange(before.toEpochMilli() - length.toMillis(), before.toEpochMilli());
    }


    public static TimeRange prev(Period length) {
        return prev(length, Instant.ofEpochMilli(EventTime.unix()));
    }

    public static TimeRange prev(Period length, String beforeDateTime) {
        return prev(length, EventTime.unix(beforeDateTime));
    }

    public static TimeRange prev(Period length, Long beforeEpochMillis) {
        return prev(length, Instant.ofEpochMilli(beforeEpochMillis));
    }

    public static TimeRange prev(Period length, Instant before) {
        return new TimeRange(Instant.from(length.subtractFrom(before)).toEpochMilli(), before.toEpochMilli());
    }


    public static TimeRange next(Duration length) {
        return next(length, Instant.ofEpochMilli(EventTime.unix()));
    }

    public static TimeRange next(Duration length, String afterDatetime) {
        return next(length, EventTime.unix(afterDatetime));
    }

    public static TimeRange next(Duration length, Long afterEpochMillis) {
        return next(length, Instant.ofEpochMilli(afterEpochMillis));
    }

    public static TimeRange next(Duration length, Instant after) {
        return new TimeRange(after.toEpochMilli(), Instant.from(length.addTo(after)).toEpochMilli());
    }

    public static TimeRange next(Period length) {
        return next(length, Instant.ofEpochMilli(EventTime.unix()));
    }

    public static TimeRange next(Period length, String afterDateTime) {
        return next(length, EventTime.unix(afterDateTime));
    }

    public static TimeRange next(Period length, Long afterEpochMillis) {
        return next(length, Instant.ofEpochMilli(afterEpochMillis));
    }

    public static TimeRange next(Period length, Instant after) {
        return new TimeRange(after.toEpochMilli(), Instant.from(length.addTo(after)).toEpochMilli());
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
        this.start = Math.max(0, startEpochMs);
        this.end = Math.max(start, endEpochMs);
        this.duration = endEpochMs - startEpochMs;
    }

}
