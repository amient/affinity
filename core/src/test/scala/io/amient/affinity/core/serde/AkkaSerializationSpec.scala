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

import akka.actor.{Actor, ActorSystem, Props}
import akka.serialization.SerializationExtension
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.avro.MemorySchemaRegistry
import io.amient.affinity.avro.record.AvroRecord
import io.amient.affinity.core.actor.{CreateKeyValueMediator, KeyValueMediatorCreated, RegisterMediatorSubscriber, Routed}
import io.amient.affinity.core.serde.collection.SeqSerde
import io.amient.affinity.core.serde.primitive.OptionSerde
import io.amient.affinity.core.util.{Reply, Scatter}
import io.amient.affinity.{AffinityActorSystem, Conf}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.collection.immutable.Seq

case class Key(key: Int) extends AvroRecord with Routed with Reply[Option[TestValue]]

case class NonAvroCase(key: Int)

case class TestValue(items: List[Int]) extends AvroRecord {
  def withAddedItem(item: Int) = TestValue(items :+ item)
  def withRemovedItem(item: Int) = TestValue(items.filter(_ != item))
}

case class CountMsgAvro() extends AvroRecord  with Scatter[Long] {
  override def gather(r1: Long, r2: Long): Long = r1 + r2
}

class RefActor extends Actor {
  override def receive: Receive = {
    case _ => ()
  }
}

class AkkaSerializationSpec extends WordSpecLike with BeforeAndAfterAll with Matchers {

  val registry = new MemorySchemaRegistry()

  val system: ActorSystem = AffinityActorSystem.create(ConfigFactory.load("akkaserializationspec"))

  val ref = system.actorOf(Props[RefActor])

  override protected def afterAll(): Unit = system.terminate()

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
      val in = NonAvroCase(123)
      val bytes = SerializationExtension(system).serialize(in).get
      SerializationExtension(system).deserialize(bytes, classOf[NonAvroCase]).get should be (in)
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

    "can be constructed without actor system context" in {
      Serde.of[List[Long]](ConfigFactory.empty.withValue(
        Conf.Affi.Avro.Class.path, ConfigValueFactory.fromAnyRef(classOf[MemorySchemaRegistry].getName)))
        .isInstanceOf[SeqSerde] should be(true)
    }

    "serialize correctly when elements are simple case classes" in {
      assert(classOf[java.io.Serializable].isAssignableFrom(classOf[NonAvroCase]))
      val x: List[NonAvroCase] = NonAvroCase(3) :: List(NonAvroCase(1), NonAvroCase(2))
      val y: Array[Byte] = SerializationExtension(system).serialize(x).get
      val z: Seq[Key] = SerializationExtension(system).deserialize(y, classOf[List[Key]]).get
      z should be(x)
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

  "Parameterless Scatter message with AvroRecord" must {
    "be serializable" in {
      val x = CountMsgAvro()
      val y: Array[Byte] = SerializationExtension(system).serialize(x).get
      val z = SerializationExtension(system).deserialize(y, classOf[CountMsgAvro]).get
      z should be(x)
    }
  }

  "Internal message" must {
    "serialize efficiently CreateKeyValueMediator message" in {
      val x = CreateKeyValueMediator("hello", 1000)
      val y: Array[Byte] = SerializationExtension(system).serialize(x).get
      y.mkString(".") should be ("1.0.5.104.101.108.108.111.0.11.0.0.0.-56.0.0.0.0.2.-48.15")
      val z = SerializationExtension(system).deserialize(y, classOf[CreateKeyValueMediator]).get
      z should be(x)
    }

    "serialize efficiently KeyValueMediatorCreated message" in {
      val x = KeyValueMediatorCreated(ref)
      val y: Array[Byte] = SerializationExtension(system).serialize(x).get
      y.mkString(".") should startWith ("2.")
      val z = SerializationExtension(system).deserialize(y, classOf[KeyValueMediatorCreated]).get
      z should be(x)
    }

    "serialize efficiently RegisterMediatorSubscriber message" in {
      val x = RegisterMediatorSubscriber(ref)
      val y: Array[Byte] = SerializationExtension(system).serialize(x).get
      y.mkString(".") should startWith ("3.")
      val z = SerializationExtension(system).deserialize(y, classOf[RegisterMediatorSubscriber]).get
      z should be(x)
    }

  }
}
