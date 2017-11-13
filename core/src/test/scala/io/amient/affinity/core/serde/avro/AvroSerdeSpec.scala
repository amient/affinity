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

package io.amient.affinity.core.serde.avro

import io.amient.affinity.avro.{AvroRecord, AvroSerde}
import io.amient.affinity.core.TestAvroSerde
import io.amient.affinity.avro.schema.MemorySchemaRegistry
import org.scalatest.{FlatSpec, Matchers}

class AvroSerdeSpec extends FlatSpec with Matchers {

  /**
    * Data version 1 is written at some point in the past
    */
  val oldSerde = new MemorySchemaRegistry {
    register(classOf[_V1_Composite])

    //Future schema V2 available in old version - slightly unrealistic for embedded registry but here we're testing
    //the API for handling serialization, not the embedded implementation of registry, i.e. this would be true
    //in case of shared registry, like zookeeper of kafka-based, or confluent schema registry
    register(classOf[_V1_Composite], AvroRecord.inferSchema(classOf[Composite]))
    register(classOf[Base])
    initialize()

    override def close(): Unit = ()
  }

  /**
    * New version 2 of the case class and schema is added at the end of registry
    * the previous V1 schema version now points to the newest case class.
    */
  val newSerde = new TestAvroSerde
  newSerde.initialize()

  "Data written with an older serde" should "be rendered into the current representation in a backward-compatible way" in {
    val oldValue = oldSerde.toBytes(_V1_Composite(Seq(Base(ID(1), Side.LEFT)), 10))
    val renderedValue = newSerde.fromBytes(oldValue)
    renderedValue should be(Composite(Seq(Base(ID(1), Side.LEFT)), Map()))
  }

  "Data Written with a newer serde" should "be rendered into the the current representation in a forward-compatible way" in {
    val newValue = newSerde.toBytes(Composite(Seq(Base(ID(1), Side.LEFT)), Map("1" -> Base(ID(1), Side.LEFT))))
    val downgradedValue = oldSerde.fromBytes(newValue)
    downgradedValue should be(_V1_Composite(Seq(Base(ID(1), Side.LEFT)), 0))
  }

  "Primitive types" should "be handled directly without registration" in {
    newSerde.fromBytes(newSerde.toBytes(true)) should equal(true)
    newSerde.fromBytes(newSerde.toBytes(100)) should equal(100)
    newSerde.fromBytes(newSerde.toBytes(100L)) should equal(100L)
    newSerde.fromBytes(newSerde.toBytes(1.0f)) should equal(1.0f)
    newSerde.fromBytes(newSerde.toBytes(10.01)) should equal(10.01)
    newSerde.fromBytes(newSerde.toBytes("hello")) should equal("hello")
  }

  "Scala Enums" should "be treated as EnumSymbols" in {
    val serialized = newSerde.toBytes(AvroEnums())
    newSerde.fromBytes(serialized) should be (AvroEnums())
    val a = AvroEnums(Side.RIGHT, Some(Side.RIGHT), None, List(Side.LEFT, Side.RIGHT), List(None, Some(Side.RIGHT)))
    val as = newSerde.toBytes(a)
    newSerde.fromBytes(as) should be (a)
  }

  "Optional types" should "be treated as union(null, X)" in {
    val emptySerialized = newSerde.toBytes(AvroPrmitives())
    val empty = newSerde.fromBytes(emptySerialized)
    empty should equal(AvroPrmitives())
    val nonEmpty = AvroPrmitives(
      None, Some(true),
      None, Some(Int.MinValue),
      None, Some(Long.MaxValue),
      None, Some(Float.MaxValue),
      None, Some(Double.MaxValue),
      None, Some("Hello")
    )
    val nonEmptySerialized = newSerde.toBytes(nonEmpty)
    newSerde.fromBytes(nonEmptySerialized) should equal(nonEmpty)
  }

  "Case class types" should "be treated as Named Types" in {
    val emptySerialized = newSerde.toBytes(AvroNamedRecords())
    val empty = newSerde.fromBytes(emptySerialized)
    empty should equal(AvroNamedRecords())

    val a = AvroNamedRecords(ID(99), Some(ID(99)), None, List(ID(99), ID(100)), List(None, Some(ID(99)), None))
    val as = newSerde.toBytes(a)
    newSerde.fromBytes(as) should be (a)

  }

}
