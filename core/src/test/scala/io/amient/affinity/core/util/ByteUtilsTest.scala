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

import java.util.UUID

import org.scalatest.{FlatSpec, Matchers}

class ByteUtilsTest extends FlatSpec with Matchers {

  "parseRadix16" should "be reversible" in {
    val input: String = "----020ac416f90d91cffc09b56a9e7aea0420e0cf59----"
    val b: Array[Byte] = ByteUtils.parseRadix16(input.getBytes, 4, 40)
    ByteUtils.toRadix16(b, 0, 20) should be("020ac416f90d91cffc09b56a9e7aea0420e0cf59")
  }

  "uuid parser" should "be compatible with java.lang.UUID" in {
    //val uuid = UUID.randomUUID()
    val uuid = UUID.fromString("1c901ed0-b8a1-43ff-ae8e-8e870f603743")
    val parsed1 = ByteUtils.parseUUID(uuid.toString)
    val parsed2: Array[Byte] = ByteUtils.uuid(uuid)
    parsed1 should equal(parsed2)

    val converted1: UUID = ByteUtils.uuid(parsed1)
    val converted2: UUID = ByteUtils.uuid(parsed2)
    converted1 should equal(converted2)
    converted2 should equal(uuid)

  }

}
