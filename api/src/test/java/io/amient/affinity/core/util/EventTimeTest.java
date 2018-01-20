package io.amient.affinity.core.util;

import org.junit.Test;

import java.time.OffsetDateTime;
import java.time.ZoneId;

import static org.junit.Assert.assertEquals;

public class EventTimeTest {

    long unix = 1516456801000L;

    @Test
    public void testParsers() {
        assertEquals(unix, EventTime.unix("20.01.2018 16:00:01+02"));
        assertEquals(unix, EventTime.unix("2018-01-20T16:00:01+02:00"));
    }

    @Test
    public void testLocal() {
        ZoneId zone = ZoneId.of("Europe/Vilnius");
        OffsetDateTime local = EventTime.local(unix, zone);
        assertEquals("20.01.2018 16:00:01+02", local.format(EventTime.format1));
        assertEquals("2018-01-20T16:00:01+02:00", local.format(EventTime.format2));

    }
}
