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
import java.security.SecureRandom;
import java.util.Arrays;

public abstract class JavaCryptoProof {

    static private java.util.Random random = new SecureRandom();

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

    abstract protected byte[] sign(byte[] arg) throws Exception;

    public String sign(String arg) throws Exception {
        return toHex(sign(arg.getBytes("UTF-8")));
    }

    public void verify(String hash, byte[] arg) throws Exception {
        assert (toHex(sign(arg)) == hash);
    }

    public void verify(String hash, String arg) throws Exception {
        verify(hash, arg.getBytes("UTF-8"));
    }
}
