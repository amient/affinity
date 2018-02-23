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
