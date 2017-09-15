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
import com.typesafe.config.ConfigFactory
import io.amient.affinity.core.IntegrationTestBase
import io.amient.affinity.core.serde.collection.SeqSerde
import io.amient.affinity.core.serde.primitive.{OptionSerde, StringSerde}
import io.amient.affinity.core.transaction.TestKey
import org.scalatest.Matchers

import scala.collection.immutable.Seq

class WrapSerdesSpec extends IntegrationTestBase with Matchers {


  "OptionSerde" must {

    val serde = SerializationExtension(system).serializerOf(classOf[OptionSerde].getName).get

    "work with with None" in {
      val bytes = serde.toBinary(None)
      bytes.length should equal(0)
      serde.fromBinary(bytes) should be(None)
    }
    "work with with wrapped string" in {
      val stringSerde = SerializationExtension(system).serializerOf(classOf[StringSerde].getName).get
      val string = stringSerde.toBinary("XYZ")
      string.mkString(".") should equal("88.89.90")
      stringSerde.fromBinary(string) should be("XYZ")

      val bytes = serde.toBinary(Some("XYZ"))
      bytes.mkString(".") should equal("0.0.0.103.88.89.90")
      serde.fromBinary(bytes) should be(Some("XYZ"))
    }
    "work with wrapped unit" in {
      val bytes = serde.toBinary(Some(()))
      bytes.mkString(".") should equal("0.0.0.100")
      serde.fromBinary(bytes) should be(Some(()))
    }
    "work with wrapped tuple" in {
      val bytes = serde.toBinary(Some(("XYZ", 10)))
      bytes.mkString(".") should equal("0.0.0.-124.0.0.0.2.0.0.0.7.0.0.0.103.88.89.90.0.0.0.8.0.0.0.101.0.0.0.10")
      serde.fromBinary(bytes) should be(Some(("XYZ", 10)))
    }
  }

  "List" must {

    "serialize correctly when elements are AvroRecords" in {
      val x: Seq[TestKey] = List(TestKey(1), TestKey(2), TestKey(3))
      val y: Array[Byte] = SerializationExtension(system).serialize(x).get
      val z: Seq[TestKey] = SerializationExtension(system).deserialize(y, classOf[List[TestKey]]).get
      z should be(x)
    }

    "be constructible from a simple Config" in {
      Serde.of[List[Long]](ConfigFactory.defaultReference()).isInstanceOf[SeqSerde] should be (true)
    }
  }

  "Set" must {

    "serialize correctly when elements are AvroRecords" in {
      val x: Set[TestKey] = Set(TestKey(1), TestKey(2), TestKey(3))
      val y: Array[Byte] = SerializationExtension(system).serialize(x).get
      val z: Set[TestKey] = SerializationExtension(system).deserialize(y, classOf[Set[TestKey]]).get
      z should be(x)
    }
  }
}
