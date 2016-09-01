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

import java.security.{MessageDigest, SecureRandom}
import javax.xml.bind.DatatypeConverter

import org.scalacheck.Gen
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

trait CryptoProof {
  protected def sign(arg: Array[Byte]): Array[Byte]
  def hash(arg: String): String = CryptoProof.toHex(sign(arg.getBytes("UTF-8")))
  def sign(arg: String): String = CryptoProof.toHex(sign(arg.getBytes("UTF-8")))
  def verify(hash: String, arg:String): Unit = verify(hash, arg.getBytes("UTF-8"))
  def verify(hash: String, arg:Array[Byte]): Unit = assert(CryptoProof.toHex(sign(arg)) == hash)
}

object CryptoProof {
  private val random = new SecureRandom()
  def toHex(bytes: Array[Byte]): String = DatatypeConverter.printHexBinary(bytes)
  def fromHex(hex: String): Array[Byte] =  DatatypeConverter.parseHexBinary(hex)
  def generateSalt(): Array[Byte] = {
    val bytes = Array.fill[Byte](16)(0)
    random.nextBytes(bytes)
    bytes
  }
}

class CryptoProofSHA256(val salt: Array[Byte]) extends CryptoProof {
  def this(hexSalt: String) = this(CryptoProof.fromHex(hexSalt))
  protected def sign(arg: Array[Byte]): Array[Byte] = {
    val digest = MessageDigest.getInstance("SHA-256")
    val input:Array[Byte] = Array.fill(salt.length + arg.length)(0)
    Array.copy(salt, 0, input, 0, salt.length)
    Array.copy(arg, 0, input, salt.length, arg.length)
    digest.digest(input)
  }
}

class CryptoProofSpec extends PropSpec with PropertyChecks with Matchers {

  val salts: Gen[Array[Byte]] = for {
    salt <- Gen.choose(Integer.MIN_VALUE + 1, Integer.MAX_VALUE)
  } yield CryptoProof.generateSalt

  val hexSalts: Gen[String] = for {
    salt <- salts
  } yield CryptoProof.toHex(salt)


  property("slat bytes to hex conversion is reversible") {
    forAll(salts) { salt =>
      val hexSalt = CryptoProof.toHex(salt)
      val reversedSalt = CryptoProof.fromHex(hexSalt)
      reversedSalt should equal(salt)
    }
  }

  property("CryptoProofSHA256 holds for all salts and args") {
    forAll(salts, Gen.alphaStr) { (salt, arg) =>
      val proof = new CryptoProofSHA256(salt)
      val signature = proof.sign(arg)
      val hexProof = new CryptoProofSHA256(CryptoProof.toHex(salt))
      val hexSignature = hexProof.sign(arg)
      signature should equal (hexSignature)
      proof.verify(signature, arg)
      hexProof.verify(signature, arg)
      proof.verify(hexSignature, arg)
      hexProof.verify(hexSignature, arg)
    }
  }

}