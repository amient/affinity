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
import io.amient.affinity.Conf
import io.amient.affinity.avro.MemorySchemaRegistry
import io.amient.affinity.avro.record.AvroRecord
import io.amient.affinity.core.IntegrationTestBase
import io.amient.affinity.core.actor.{CreateKeyValueMediator, Routed}
import io.amient.affinity.core.serde.collection.SeqSerde
import io.amient.affinity.core.serde.primitive.OptionSerde
import io.amient.affinity.core.util.Reply

import scala.collection.immutable.Seq

case class Key(key: Int) extends AvroRecord with Routed with Reply[Option[TestValue]]

case class TestValue(items: List[Int]) extends AvroRecord {
  def withAddedItem(item: Int) = TestValue(items :+ item)
  def withRemovedItem(item: Int) = TestValue(items.filter(_ != item))
}

class AkkaSerializationSpec extends IntegrationTestBase {

  val registry = new MemorySchemaRegistry()

  "Akka-serialized AvroRecord bytes" must {
    "be identical to AvroSerde bytes - this is important for murmur2 hash partitioner" in {
      val in = Key(1)
      val bytes = SerializationExtension(system).serialize(in).get
      val bytes2 = registry.toBytes(in)
      bytes.mkString(".") should be(bytes2.mkString("."))
    }
  }

  "Akka-serialized String bytes" must {
    "be identical to AvroSerde bytes - this is important for murmur2 hash partitioner" in {
      val in = "test-string"
      val bytes = SerializationExtension(system).serialize(in).get
      val bytes2 = registry.toBytes(in)
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
      val in = CreateKeyValueMediator("state", TestValue(List(1,2,3)))
      val bytes = SerializationExtension(system).serialize(in).get
      bytes.mkString(".") should be("-84.-19.0.5.115.114.0.52.105.111.46.97.109.105.101.110.116.46.97.102.102.105.110." +
        "105.116.121.46.99.111.114.101.46.97.99.116.111.114.46.67.114.101.97.116.101.75.101.121.86.97.108.117.101.77.101" +
        ".100.105.97.116.111.114.70.-112.7.63.-12.-16.-81.66.2.0.2.76.0.3.107.101.121.116.0.18.76.106.97.118.97.47.108" +
        ".97.110.103.47.79.98.106.101.99.116.59.76.0.10.115.116.97.116.101.83.116.111.114.101.116.0.18.76.106.97.118.97" +
        ".47.108.97.110.103.47.83.116.114.105.110.103.59.120.112.115.114.0.39.105.111.46.97.109.105.101.110.116.46.97" +
        ".102.102.105.110.105.116.121.46.99.111.114.101.46.115.101.114.100.101.46.84.101.115.116.86.97.108.117.101.3.31" +
        ".6.-127.18.123.50.26.2.0.1.76.0.5.105.116.101.109.115.116.0.33.76.115.99.97.108.97.47.99.111.108.108.101.99.116" +
        ".105.111.110.47.105.109.109.117.116.97.98.108.101.47.76.105.115.116.59.120.114.0.41.105.111.46.97.109.105.101" +
        ".110.116.46.97.102.102.105.110.105.116.121.46.97.118.114.111.46.114.101.99.111.114.100.46.65.118.114.111.82.101" +
        ".99.111.114.100.63.-18.16.47.32.-56.-52.-34.2.0.0.120.112.115.114.0.50.115.99.97.108.97.46.99.111.108.108.101" +
        ".99.116.105.111.110.46.105.109.109.117.116.97.98.108.101.46.76.105.115.116.36.83.101.114.105.97.108.105.122.97" +
        ".116.105.111.110.80.114.111.120.121.0.0.0.0.0.0.0.1.3.0.0.120.112.115.114.0.17.106.97.118.97.46.108.97.110.103" +
        ".46.73.110.116.101.103.101.114.18.-30.-96.-92.-9.-127.-121.56.2.0.1.73.0.5.118.97.108.117.101.120.114.0.16.106" +
        ".97.118.97.46.108.97.110.103.46.78.117.109.98.101.114.-122.-84.-107.29.11.-108.-32.-117.2.0.0.120.112.0.0.0.1" +
        ".115.113.0.126.0.10.0.0.0.2.115.113.0.126.0.10.0.0.0.3.115.114.0.44.115.99.97.108.97.46.99.111.108.108.101.99" +
        ".116.105.111.110.46.105.109.109.117.116.97.98.108.101.46.76.105.115.116.83.101.114.105.97.108.105.122.101.69" +
        ".110.100.36.-118.92.99.91.-9.83.11.109.2.0.0.120.112.120.116.0.5.115.116.97.116.101")
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
      Serde.of[List[Long]](ConfigFactory.empty.withValue(
        Conf.Affi.Avro.Class.path, ConfigValueFactory.fromAnyRef(classOf[MemorySchemaRegistry].getName)))
        .isInstanceOf[SeqSerde] should be(true)
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
