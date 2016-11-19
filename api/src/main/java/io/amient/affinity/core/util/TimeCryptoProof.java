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

package io.amient.affinity.core.util;

import javax.xml.bind.DatatypeConverter;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;

public abstract class TimeCryptoProof {

    static private java.util.Random random = new SecureRandom();
    private final byte[] salt;

    /**
     * convert bytes to hexadecimal representation
     * @param bytes input bytes
     * @return result hexadecimal number
     */
    static public String toHex(byte[] bytes) {
        return DatatypeConverter.printHexBinary(bytes);
    }

    /**
     * convert hexadecimal representation to bytes
     * @param hex input
     * @return byte array
     */
    static public byte[] fromHex(String hex) {
        return DatatypeConverter.parseHexBinary(hex);
    }

    /**
     * generate a random secure salt
     * @return byte array salt
     */
    static public byte[] generateSalt() {
        byte[] bytes = new byte[16];
        Arrays.fill(bytes, (byte) 0);
        random.nextBytes(bytes);
        return bytes;
    }

    static private long utcInWholeMinutes(int offset) {
        return ZonedDateTime.now(ZoneOffset.UTC).withNano(0).withSecond(0).toEpochSecond() + offset * 60;
    }

    /**
     * Construct a Thread-Safe instance of Crypto with a given salt
     * @param salt byte array salt
     */
    public TimeCryptoProof(byte[] salt) {
        this.salt = salt;
    }

    /**
     * Construct a Thread-Safe instance of Crypto with a given salt
     * @param hexSalt hexadecimal salt
     */
    public TimeCryptoProof(String hexSalt) {
        this.salt = fromHex(hexSalt);
    }

    /**
     * Sign a given input argument
     * @param arg input string to sign
     * @return signature string
     * @throws Exception if anything goes wrong
     */
    final public String sign(String arg) throws Exception {
        return toHex(sign(arg.getBytes("UTF-8")));
    }

    /**
     * Sign a given input argument
     * @param arg byte array input
     * @return byte array signature
     * @throws Exception if anything goes wrong
     */
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


    final public boolean verify(String signature, String arg) throws Exception {
        return verify(fromHex(signature), arg.getBytes("UTF-8"));
    }

    final public boolean verify(byte[] signature, byte[] arg) throws Exception {
        return (Arrays.equals(sign(arg, utcInWholeMinutes(0)), signature))
                || (Arrays.equals(sign(arg, utcInWholeMinutes(-1)), signature))
                || (Arrays.equals(sign(arg, utcInWholeMinutes(+1)), signature));
    }

    abstract protected byte[] hash(byte[] input) throws Exception;


}
