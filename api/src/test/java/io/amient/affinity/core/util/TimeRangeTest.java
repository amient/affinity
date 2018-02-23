/*
 * Copyright 2016-2018 Michal Harish, michal.harish@gmail.com
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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