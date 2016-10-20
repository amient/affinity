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

package io.amient.affinity.core.serde

import akka.serialization.SerializationExtension
import io.amient.affinity.core.ActorUnitTestBase
import io.amient.affinity.core.serde.primitive.OptionSerde
import org.scalatest.Matchers

class WrapSerdesSpec extends ActorUnitTestBase with Matchers {


  val serde = SerializationExtension(system).serializerOf(classOf[OptionSerde].getName).get

  "OptionSerde" must {
    "work with with None" in {
      val bytes = serde.toBinary(None)
      bytes.length should equal(0)
      serde.fromBinary(bytes) should be(None)
    }
    "work with with wrapped string" in {
        val bytes = serde.toBinary(Some("XYZ"))
        bytes.mkString(".") should equal("0.0.0.21.88.89.90")
        serde.fromBinary(bytes) should be(Some("XYZ"))
    }
    "work with wrapped unit" in {
      val bytes = serde.toBinary(Some(()))
      bytes.mkString(".") should equal("0.0.0.30")
      serde.fromBinary(bytes) should be(Some(()))
    }
    "work with wrapped tuple" in {
      val bytes = serde.toBinary(Some(("XYZ", 10)))
      bytes.mkString(".") should equal("0.0.0.31.0.0.0.2.0.0.0.7.0.0.0.21.88.89.90.0.0.0.8.0.0.0.20.0.0.0.10")
      serde.fromBinary(bytes) should be(Some(("XYZ", 10)))
    }
  }
}
