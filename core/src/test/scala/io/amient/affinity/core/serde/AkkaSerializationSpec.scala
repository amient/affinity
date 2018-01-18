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
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.avro.schema.MemorySchemaRegistry
import io.amient.affinity.avro.{AvroRecord, AvroSerde}
import io.amient.affinity.core.IntegrationTestBase
import io.amient.affinity.core.actor.Partition.CreateKeyValueMediator
import io.amient.affinity.core.actor.Routed
import io.amient.affinity.core.http.TestValue
import io.amient.affinity.core.serde.collection.SeqSerde
import io.amient.affinity.core.serde.primitive.OptionSerde
import io.amient.affinity.core.util.Reply
import org.scalatest.Matchers

import scala.collection.immutable.Seq

case class Key(key: Int) extends AvroRecord with Routed with Reply[Option[TestValue]]

class AkkaSerializationSpec extends IntegrationTestBase with Matchers {

  "Akka-serialized AvroRecord bytes" must {
    "be identical to AvroSerde bytes - this is important for murmur2 hash partitioner" in {
      val in = Key(1)
      val bytes = SerializationExtension(system).serialize(in).get
      val bytes2 = new MemorySchemaRegistry().toBytes(in)
      bytes.mkString(".") should be(bytes2.mkString("."))
    }
  }

  "Akka-serialized String bytes" must {
    "be identical to AvroSerde bytes - this is important for murmur2 hash partitioner" in {
      val in = "test-string"
      val bytes = SerializationExtension(system).serialize(in).get
      val bytes2 = new MemorySchemaRegistry().toBytes(in)
      bytes.mkString(".") should be(bytes2.mkString("."))
    }
  }

  "TupleSerde" must {
    "work with wrapped tuple3" in {
      val in = (1000, 1.1, "graph")
      val bytes = SerializationExtension(system).serialize(in).get
      val out = SerializationExtension(system).deserialize(bytes, classOf[Tuple3[Int, Double, String]]).get
      out should be(in)
    }

  }

  "Java serializer" must {
    "work with internal messages" in {
      val in = CreateKeyValueMediator("state", 1)
      val bytes = SerializationExtension(system).serialize(in).get
      println(bytes.mkString(".")) //TODO assert this is the case
    }
  }

  "OptionSerde" must {

    val serde = SerializationExtension(system).serializerOf(classOf[OptionSerde].getName).get

    "work with with None" in {
      val bytes = serde.toBinary(None)
      bytes.length should equal(0)
      serde.fromBinary(bytes) should be(None)
    }
    "work with with wrapped string" in {
      val stringSerde = SerializationExtension(system).serializerFor(classOf[String])
      val string = stringSerde.toBinary("XYZ")
      string.mkString(".") should equal("0.0.0.0.6.6.88.89.90")
      stringSerde.fromBinary(string) should be("XYZ")

      val bytes = serde.toBinary(Some("XYZ"))
      bytes.mkString(".") should equal("0.0.0.-56.0.0.0.0.6.6.88.89.90")
      serde.fromBinary(bytes) should be(Some("XYZ"))
    }
    "work with wrapped unit" in {
      val bytes = serde.toBinary(Some(()))
      bytes.mkString(".") should equal("0.0.0.100")
      serde.fromBinary(bytes) should be(Some(()))
    }
    "work with wrapped tuple" in {
      val bytes = serde.toBinary(Some(("XYZ", 10)))
      bytes.mkString(".") should equal("0.0.0.-124.0.0.0.2.0.0.0.13.0.0.0.-56.0.0.0.0.6.6.88.89.90.0.0.0.10.0.0.0.-56.0.0.0.0.2.20")
      serde.fromBinary(bytes) should be(Some(("XYZ", 10)))
    }
  }

  "List" must {

    "serialize correctly when elements are AvroRecords" in {
      val x: Seq[Key] = List(Key(1), Key(2), Key(3))
      val y: Array[Byte] = SerializationExtension(system).serialize(x).get
      val z: Seq[Key] = SerializationExtension(system).deserialize(y, classOf[List[Key]]).get
      z should be(x)
    }

    "be constructible from a simple Config" in {
      Serde.of[List[Long]](ConfigFactory.defaultReference.withValue(
        new AvroSerde.Conf().Avro.Class.path, ConfigValueFactory.fromAnyRef(classOf[MemorySchemaRegistry].getName)))
        .isInstanceOf[SeqSerde] should be (true)
    }
  }

  "Set" must {

    "serialize correctly when elements are AvroRecords" in {
      val x: Set[Key] = Set(Key(1), Key(2), Key(3))
      val y: Array[Byte] = SerializationExtension(system).serialize(x).get
      val z: Set[Key] = SerializationExtension(system).deserialize(y, classOf[Set[Key]]).get
      z should be(x)
    }
  }
}
