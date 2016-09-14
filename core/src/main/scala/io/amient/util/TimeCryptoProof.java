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

package io.amient.util;

import javax.xml.bind.DatatypeConverter;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;

public abstract class TimeCryptoProof {

    static private java.util.Random random = new SecureRandom();
    private final byte[] salt;

    static public String toHex(byte[] bytes) {
        return DatatypeConverter.printHexBinary(bytes);
    }

    static public byte[] fromHex(String hex) {
        return DatatypeConverter.parseHexBinary(hex);
    }

    static public byte[] generateSalt() {
        byte[] bytes = new byte[16];
        Arrays.fill(bytes, (byte) 0);
        random.nextBytes(bytes);
        return bytes;
    }

    static public long utcInWholeMinutes(int offset) {
        return ZonedDateTime.now(ZoneOffset.UTC).withNano(0).withSecond(0).toEpochSecond() + offset * 60;
    }

    public TimeCryptoProof(byte[] salt) {
        this.salt = salt;
    }

    public TimeCryptoProof(String hexSalt) {
        this.salt = fromHex(hexSalt);
    }

    final public String sign(String arg) throws Exception {
        return toHex(sign(arg.getBytes("UTF-8")));
    }

    final public byte[] sign(byte[] arg) throws Exception {
        return sign(arg, utcInWholeMinutes(0));
    }

    private byte[] sign(byte[] arg, long utc) throws Exception {
        ByteBuffer in = ByteBuffer.allocate(salt.length + 8 + arg.length);
        in.put(salt);
        in.putLong(utc);
        in.put(arg);
        in.flip();
        return hash(in.array());
    }


    final public void verify(String signature, String arg) throws Exception {
        verify(fromHex(signature), arg.getBytes("UTF-8"));
    }

    final public void verify(byte[] signature, byte[] arg) throws Exception {
        assert (sign(arg, utcInWholeMinutes(0)) == signature)
                || (sign(arg, utcInWholeMinutes(-1)) == signature)
                || sign(arg, utcInWholeMinutes(+1)) == signature;
    }

    abstract protected byte[] hash(byte[] input) throws Exception;


}
