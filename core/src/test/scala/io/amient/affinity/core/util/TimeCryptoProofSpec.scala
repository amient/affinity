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

package io.amient.affinity.core.util

import java.net.URL

import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class TimeCryptoProofSpec extends PropSpec with PropertyChecks with Matchers {

  val salts: Gen[Array[Byte]] = for {
    salt <- Gen.choose(Integer.MIN_VALUE + 1, Integer.MAX_VALUE)
  } yield TimeCryptoProof.generateSalt

  val hexSalts: Gen[String] = for {
    salt <- salts
  } yield TimeCryptoProof.toHex(salt)


  property("slat bytes to hex conversion is reversible") {
    forAll(salts) { salt =>
      val hexSalt = TimeCryptoProof.toHex(salt)
      val reversedSalt = TimeCryptoProof.fromHex(hexSalt)
      reversedSalt should equal(salt)
    }
  }

  property("CryptoProofSHA256 holds for all salts and args") {
    forAll(salts, Gen.alphaStr) { (salt, arg) =>

      val crypto = new TimeCryptoProofSHA256(salt)
      val signature: String = crypto.sign(arg)

      TimeCryptoProof.toHex(salt)
      val hexProof = new TimeCryptoProofSHA256(salt)
      val hexSignature = hexProof.sign(arg)
      signature should equal(hexSignature)
      try {
        assert(crypto.verify(signature, arg) == true)
        assert(hexProof.verify(signature, arg) == true)
        assert(crypto.verify(hexSignature, arg) == true)
        assert(hexProof.verify(hexSignature, arg) == true)
      } catch {
        case e: Throwable => e.printStackTrace(); throw e
      }
    }
  }

  property("example function is consistent with the implementation") {
    forAll(hexSalts, Gen.alphaStr) { (hexSalt, arg) =>
      val crypto = new TimeCryptoProofSHA256(hexSalt)
      try {
        assert(crypto.sign(arg) == crypto.timeBasedHash(arg, hexSalt, 0) == true)
        assert(crypto.verify(crypto.sign(arg), arg) == true)
        assert(crypto.verify(crypto.timeBasedHash(arg, hexSalt, 0), arg) == true)
      } catch {
        case e: Throwable => e.printStackTrace(); throw e
      }
    }
  }

  property("example function generates different signatures for different salts") {
    forAll(hexSalts, hexSalts) { case (salt1, salt2) =>
      whenever(salt1 != salt2) {
        val crypto1 = new TimeCryptoProofSHA256(salt1)
        val crypto2 = new TimeCryptoProofSHA256(salt2)
        val url = new URL("https://example.com/xyz?param=123456")
        val urlNoQuery = new URL("https://example.com/xyz")
        val sig1 = crypto1.sign(url.getPath)
        val sig1NoQuery = crypto1.sign(urlNoQuery.getPath)
        val sig2 = crypto2.sign(url.toString)
        val sig2NoQuery = crypto2.sign(urlNoQuery.toString)
        assert(sig1 != sig2)
        assert(sig1NoQuery != sig2NoQuery)
      }
    }
  }

}