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

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public interface EventTime {

    /**
     * event time since the unix epoch Jan 01 00:00:00 UTC
     * this timestamp should not be influenced by local time zone
     *
     * @return long milliseconds
     */
    long eventTimeUnix();

    /**
     * @return event time translated into LocalDate instance
     */
    default OffsetDateTime eventTimeLocal() {
        return local(eventTimeUnix());
    }

    /**
     * @param unix milliseconds since the unix epoch
     * @return LocalDate instance for the given utc timestamp
     */
    static OffsetDateTime local(long unix) {
        return local(unix, ZoneId.systemDefault());
    }

    /**
     * @param unix milliseconds since the unix epoch
     * @param zone zone which is represents the local
     * @return LocalDate instance for the given utc timestamp and zone
     */
    static OffsetDateTime local(long unix, ZoneId zone) {
        return Instant.ofEpochMilli(unix).atZone(zone).toOffsetDateTime();
    }

    /**
     * return corresponding unix timestamp for a given local date time
     *
     * @param t local time
     * @return timestamp in milliseconds since the unix epoch
     */
    static long unix(OffsetDateTime t) {
        return t.toInstant().toEpochMilli();
    }

    /**
     * the system's current view of the time without any local time zone influence
     *
     * @return current number of milliseconds since the unix epoch Jan 01 00:00:00 UTC
     */
    static long unix() {
        return System.currentTimeMillis();
    }

    /**
     * convert local time to a corresponding unix timestamp
     * @param localTime local date time in one of the following formats listed below
     * <p>
     *
     * - Primary ISO formats:
     *  - 2018-01-20T16:00:01+02:00
     *  - 2018-01-20T16:00:01.123+02:00
     *  - 2018-01-20T16:00:01.123+02:00:00
     *
     * - Secondary written form with zone offset:	    20.01.2018 16:00:01+02
     * @return number of milliseconds since the unix epoch
     */
    static long unix(String localTime) {
        OffsetDateTime parsed;
        try {
            parsed = OffsetDateTime.parse(localTime, format2);
        } catch (DateTimeParseException e) {
            parsed = OffsetDateTime.parse(localTime, format1);
        }
        return unix(parsed);
    }
    DateTimeFormatter format1 = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ssx"); //20.01.2018 16:00:01+02
    DateTimeFormatter format2 = DateTimeFormatter.ISO_OFFSET_DATE_TIME;              //2018-01-20T16:00:01+02:00


    DateTimeFormatter localFormat = DateTimeFormatter.ofPattern("YYYY-MM-DD HH:mm:ss"); //2018-01-20T16:00:01+0200

    static long unix(String time, ZoneOffset offset) {
        return unix(LocalDateTime.parse(time, localFormat).atOffset(offset));
    }

}
