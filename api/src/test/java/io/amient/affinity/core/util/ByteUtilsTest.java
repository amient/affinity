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

import java.util.Arrays;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ByteUtilsTest {
    @Test
    public void parseRadix16ShouldBeReversible() {
        String input = "----020ac416f90d91cffc09b56a9e7aea0420e0cf59----";
        byte[] b = ByteUtils.parseRadix16(input.getBytes(), 4, 40);
        assertEquals("020ac416f90d91cffc09b56a9e7aea0420e0cf59", ByteUtils.toRadix16(b, 0, 20));
    }

    @Test

    public void uuidParserShouldBeCompatibleWithJavaUUID () {
        UUID uuid = UUID.fromString("1c901ed0-b8a1-43ff-ae8e-8e870f603743");
        byte[] parsed1 = ByteUtils.parseUUID(uuid.toString());
        byte[] parsed2 = ByteUtils.uuid(uuid);
        assertTrue(Arrays.equals(parsed2, parsed1));

        UUID converted1 = ByteUtils.uuid(parsed1);
        UUID converted2 = ByteUtils.uuid(parsed2);
        assertEquals(converted1, converted2);
        assertEquals(converted2, uuid);

    }
}
