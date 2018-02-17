/*
 * Copyright 2016 Michal Harish, michal.harish@gmail.com
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

package io.amient.affinity.core.storage;

import io.amient.affinity.core.util.ByteUtils;

import java.io.Serializable;
import java.util.Arrays;

public class ByteKey implements Serializable {

    public final byte[] bytes;

    private final int hashCode;

    public ByteKey(byte[] bytes) {
        this.bytes = bytes;
        this.hashCode = ByteUtils.murmur2(bytes);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ByteKey && hashCode == (((ByteKey) o).hashCode) && Arrays.equals(bytes, ((ByteKey) o).bytes);
    }
}
