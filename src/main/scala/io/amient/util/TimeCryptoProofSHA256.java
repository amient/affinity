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
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TimeCryptoProofSHA256 extends TimeCryptoProof {

    public TimeCryptoProofSHA256(byte[] salt) {
        super(salt);
    }

    public TimeCryptoProofSHA256(String hexSalt) {
        super(hexSalt);
    }

    @Override
    protected byte[] hash(byte[] input) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        return digest.digest(input);
    }

    /**
     * Example standalone hash function
     */
    public String timeBasedHash(String arg, String hexSalt, int shift) throws Exception {
        byte[] salt = DatatypeConverter.parseHexBinary(hexSalt);
        long utcWholeMinute = ZonedDateTime.now(ZoneOffset.UTC)
                .withNano(0).withSecond(0).toEpochSecond() + shift * 60;
        byte[] argBytes = arg.getBytes();
        ByteBuffer in = ByteBuffer.allocate(salt.length + 8 + argBytes.length);
        in.put(salt);
        in.putLong(utcWholeMinute);
        in.put(argBytes);
        in.flip();
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(in.array());
        String hexHash = DatatypeConverter.printHexBinary(hash);
        return hexHash;
    }

    /**
     * Example sign URL function
     *
     * @param url
     * @param apiKey
     * @param apiSalt
     * @return
     * @throws Exception
     */
    public URL signURL(URL url, String apiKey, String apiSalt, int shift) throws Exception {
        String signature = timeBasedHash(url.getPath().toString(), apiSalt, shift);
        String params = url.getQuery() == null ? "?" : "?" + url.getQuery() + "&";
        return new URL(url, params + "signature=" + apiKey + ":" + signature);
    }

//    public void verifiedApiCall(URL url, String apiKey, String apiSalt) throws Exception {
//        String signature = timeBasedHash(url.getPath().toString(), apiSalt, 0);
//        String params = url.getQuery() == null ? "?" : "?" + url.getQuery() + "&";
//        URL request = new URL(url, params + "signature=" + apiKey + ":" + signature);
//        HttpResponse response = http.request(request);
//        JsonNode json = jsonMapper.readValue(response.entity);
//        String responseSignature = json.get("signature");
//        assert (timeBasedHash(signature, apiSalt, 0) == responseSignature)
//                || timeBasedHash(signature, apiSalt, -1) == responseSignature
//                || timeBasedHash(signature, apiSalt, +1) == responseSignature;
//        return json;
//    }

}
