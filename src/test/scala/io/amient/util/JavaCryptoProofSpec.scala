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

package io.amient.util

import java.net.URL

import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class JavaCryptoProofSpec extends PropSpec with PropertyChecks with Matchers {

  val salts: Gen[Array[Byte]] = for {
    salt <- Gen.choose(Integer.MIN_VALUE + 1, Integer.MAX_VALUE)
  } yield JavaCryptoProof.generateSalt

  val hexSalts: Gen[String] = for {
    salt <- salts
  } yield JavaCryptoProof.toHex(salt)


  property("slat bytes to hex conversion is reversible") {
    forAll(salts) { salt =>
      val hexSalt = JavaCryptoProof.toHex(salt)
      val reversedSalt = JavaCryptoProof.fromHex(hexSalt)
      reversedSalt should equal(salt)
    }
  }

  property("CryptoProofSHA256 holds for all salts and args") {
    forAll(salts, Gen.alphaStr) { (salt, arg) =>

      val proof = new JavaCryptoProofSHA256(salt)
      val signature = proof.sign(arg)

      val hexSalt = JavaCryptoProof.toHex(salt)
      val hexProof = new JavaCryptoProofSHA256(salt)
      val hexSignature = hexProof.sign(arg)
      signature should equal (hexSignature)
      proof.verify(signature, arg)
      hexProof.verify(signature, arg)
      proof.verify(hexSignature, arg)
      hexProof.verify(hexSignature, arg)
      //println(hexSalt + " + " + arg + " -> " + signature)
    }
  }

}