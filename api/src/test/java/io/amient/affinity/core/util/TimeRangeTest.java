package io.amient.affinity.core.util;

import org.junit.Test;

import java.time.*;

import static org.junit.Assert.assertEquals;

public class TimeRangeTest {

    final long unix = 1516456801000L; //20.01.2018 16:00:01+02
    final Instant i = Instant.ofEpochMilli(unix);
    final OffsetDateTime local = EventTime.local(unix);

    @Test
    public void testLocals() {
        TimeRange range = new TimeRange(unix - 1000, unix);
        assertEquals("2018-01-20T14:00Z", range.getLocalStart().toString());
        assertEquals("2018-01-20T14:00:01Z", range.getLocalEnd().toString());
    }

    @Test
    public void testSlides() {
        TimeRange fromPeriod = new TimeRange(Period.ofDays(1), i);
        TimeRange fromDuration = new TimeRange(Duration.ofMillis(86400000), i);
        assertEquals(fromPeriod, fromDuration);
        assertEquals("2018-01-19T14:00:01Z", EventTime.local(fromPeriod.start, ZoneId.of("UTC")).toString());
    }
}