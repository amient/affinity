/*
 * Copyright 2016-2018 Michal Harish, michal.harish@gmail.com
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

package io.amient.affinity.avro

import io.amient.affinity.avro.record.{AvroRecord, AvroSerde}
import io.amient.affinity.core.util.ByteUtils
import org.apache.avro.{Schema, SchemaValidationException}
import org.scalatest.{FlatSpec, Matchers}
import org.slf4j.LoggerFactory

import scala.collection.immutable.Seq

class AvroRecordSpec extends FlatSpec with Matchers {

  private val log = LoggerFactory.getLogger(classOf[AvroRecordSpec])

  val recordV2Schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Record_V1\",\"namespace\":\"io.amient.affinity.avro\",\"fields\":[{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"SimpleRecord\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"record\",\"name\":\"SimpleKey\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]},\"default\":{\"id\":0}},{\"name\":\"side\",\"type\":{\"type\":\"enum\",\"name\":\"SimpleEnum\",\"symbols\":[\"A\",\"B\",\"C\"]},\"default\":\"A\"},{\"name\":\"seq\",\"type\":{\"type\":\"array\",\"items\":\"SimpleKey\"},\"default\":[]}]}},\"default\":[]},{\"name\":\"index\",\"type\":{\"type\":\"map\",\"values\":\"SimpleRecord\"},\"default\":{}},{\"name\":\"setOfPrimitives\",\"type\":{\"type\":\"array\",\"items\":\"long\"},\"default\":[]}]}")

  val oldSerde = new MemorySchemaRegistry {
    register[Record_V1] //Data version 1 is current from the point of view of oldSerde
    register[Record_V1](recordV2Schema) //"future" version of the Record is registered
    register[SimpleRecord]
  }

  /**
    * New version 2 of the case class and schema is added at the end of registry
    * the previous V1 schema version now points to the newest case class.
    */
  val newSerde = new MemorySchemaRegistry {
    val recordV1Schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Record_Current\",\"namespace\":\"io.amient.affinity.avro\",\"fields\":[{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"SimpleRecord\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"record\",\"name\":\"SimpleKey\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]},\"default\":{\"id\":0}},{\"name\":\"side\",\"type\":{\"type\":\"enum\",\"name\":\"SimpleEnum\",\"symbols\":[\"A\",\"B\",\"C\"]},\"default\":\"A\"},{\"name\":\"seq\",\"type\":{\"type\":\"array\",\"items\":\"SimpleKey\"},\"default\":[]}]}},\"default\":[]},{\"name\":\"removed\",\"type\":\"int\",\"default\":0}]}")
    register[Record_Current](recordV1Schema) //in new serde, v1 is the previous version
    register[Record_Current] // current version the current compile-time version of the Record
    register[SimpleRecord]
  }

  "Data written with an older serde" should "be rendered into the current representation in a backward-compatible way" in {
    val oldValue = oldSerde.toBytes(Record_V1(Seq(SimpleRecord(SimpleKey(1), SimpleEnum.A)), 10))
    oldValue.mkString(".") should be("0.0.0.0.8.2.2.0.0.0.0.20")
    val renderedValue = newSerde.fromBytes(oldValue)
    renderedValue should be(Record_Current(Seq(SimpleRecord(SimpleKey(1), SimpleEnum.A)), Map()))
  }

  "Data Written with a newer serde" should "be rendered into the the current representation in a forward-compatible way" in {
    val newValue = newSerde.toBytes(Record_Current(Seq(SimpleRecord(SimpleKey(1), SimpleEnum.A)), Map("1" -> SimpleRecord(SimpleKey(1), SimpleEnum.A))))
    newValue.mkString(",") should be("0,0,0,0,9,2,2,0,0,0,0,2,2,49,2,0,0,0,0,0")
    val downgradedValue = oldSerde.fromBytes(newValue)
    downgradedValue should be(Record_V1(Seq(SimpleRecord(SimpleKey(1), SimpleEnum.A)), 0))
  }

  "Primitive types" should "be handled directly without registration" in {
    newSerde.fromBytes(newSerde.toBytes(true)) should equal(true)
    newSerde.fromBytes(newSerde.toBytes(100)) should equal(100)
    newSerde.fromBytes(newSerde.toBytes(100L)) should equal(100L)
    newSerde.fromBytes(newSerde.toBytes(1.0f)) should equal(1.0f)
    newSerde.fromBytes(newSerde.toBytes(10.01)) should equal(10.01)
    newSerde.fromBytes(newSerde.toBytes("hello")) should equal("hello")
  }

  "Array[Byte]" should "be treated as Avro Bytes underlied by nio.ByteBuffer" in {
    def compare(x: AvroBytes, y: AvroBytes) {
      ByteUtils.equals(x.raw, y.raw) should be(true)
      ByteUtils.equals(x.optional.get, y.optional.get) should be(true)
      x.listed.zip(y.listed).foreach { case (a, b) => ByteUtils.equals(a, b) should be(true) }
    }

    val schema = AvroRecord.inferSchema[AvroBytes]
    schema.toString should be("{\"type\":\"record\",\"name\":\"AvroBytes\",\"namespace\":\"io.amient.affinity.avro\",\"fields\":[{\"name\":\"raw\",\"type\":\"bytes\"},{\"name\":\"optional\",\"type\":[\"null\",\"bytes\"]},{\"name\":\"listed\",\"type\":{\"type\":\"array\",\"items\":\"bytes\"}}]}")
    val original = AvroBytes(Array[Byte](1, 2, 3), Some(Array[Byte](6)), List(Array[Byte](10), Array[Byte](100)))
    val bytes = AvroRecord.write(original, schema)
    compare(original, AvroRecord.read[AvroBytes](bytes, schema))

    val bytes2 = newSerde.toBytes(original)
    compare(original, newSerde.fromBytes(bytes2).asInstanceOf[AvroBytes])
  }

  "Primitives used as top-level types" should "be transparently serialized" in {
    val data = Array[Byte](1, 2, 3, 4, 5)
    val schema = AvroRecord.inferSchema(data)
    schema should be(AvroRecord.BYTES_SCHEMA)
    schema.getType should be(Schema.Type.BYTES)
    AvroRecord.inferSchema[Array[Byte]] should be(schema)
    val bytes = AvroRecord.write(data, schema)
    bytes.mkString(".") should be("10.1.2.3.4.5")
    ByteUtils.equals(data, AvroRecord.read[Array[Byte]](bytes, schema)) should be(true)
    val bytes2 = AvroRecord.write(2048, AvroRecord.INT_SCHEMA)
    bytes2.mkString(".") should be("-128.32")
    AvroRecord.read[Int](bytes2, AvroRecord.INT_SCHEMA) should be(2048)
  }

  "List" should "not be used as top-level type" in {
    val value = List(1, 2, 4)
    val schema = AvroRecord.inferSchema[List[Int]]
    schema.toString should be("{\"type\":\"array\",\"items\":\"int\"}")
    val data = AvroRecord.write(value, schema)
    an[IllegalArgumentException] should be thrownBy (AvroRecord.read[List[Int]](data, schema))

  }

  "Scala Enums" should "be treated as EnumSymbols" in {
    val serialized = newSerde.toBytes(AvroEnums())
    newSerde.fromBytes(serialized) should be(AvroEnums())
    val a = AvroEnums(SimpleEnum.B, Some(SimpleEnum.B), None, List(SimpleEnum.A, SimpleEnum.B), List(None, Some(SimpleEnum.B)))
    val as = newSerde.toBytes(a)
    newSerde.fromBytes(as) should be(a)
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

    val a = AvroNamedRecords(SimpleKey(99), Some(SimpleKey(99)), None, List(SimpleKey(99), SimpleKey(100)), List(None, Some(SimpleKey(99)), None))
    val as = newSerde.toBytes(a)
    newSerde.fromBytes(as) should be(a)
  }

  "In-memory shema registry" should "reject backward-incompatible schema" in {
    val v4schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Record\",\"namespace\":\"io.amient.affinity.avro\",\"fields\":[{\"name\":\"data\",\"type\":\"string\"}]}")
    an[SchemaValidationException] should be thrownBy (newSerde.register[Record_Current](v4schema))
  }

  "AvroRecord" should "use old fields when Alias annotation is used" in {
    val schema = AvroRecord.inferSchema[AliasedAvro]
    schema.toString should be("{\"type\":\"record\",\"name\":\"AliasedAvro\",\"namespace\":\"io.amient.affinity.avro\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"aliases\":[\"old_name1\",\"old_name2\"]}]}")
  }

  it should "have minimum read/write throughput" in {
    val n = 500000
    val writeStart = System.currentTimeMillis
    for (i <- 1 to n) {
      newSerde.toBytes(SimpleRecord(SimpleKey(i), SimpleEnum.C, Seq(SimpleKey(i % 20))))
    }
    val writeEnd = System.currentTimeMillis()
    val bytes = newSerde.toBytes(SimpleRecord(SimpleKey(Int.MaxValue), SimpleEnum.C, Seq(SimpleKey(Int.MaxValue % 20)), Some(SimpleEnum.A)))
    println(s"AvroSerde Writes/Sec: ${n.toLong * 1000 / (writeEnd - writeStart)}")
    val readStart = System.currentTimeMillis
    for (i <- 1 to n) {
      newSerde.fromBytes(bytes)
    }
    val readEnd = System.currentTimeMillis()
    println(s"AvroSerde Reads/Sec: ${n.toLong * 1000 / (readEnd - readStart)}")

  }


  "Compound Key" should "have the same binary prefix as the whole key" in {
    def f(city: String, num: Int): CompoundKey = {
      val ck = new CompoundKey("UK", city, num)
      val bytes = newSerde.toBytes(ck)
      val ck_ = newSerde.fromBytes(bytes)
      ck_ should be(ck)
      ck
    }

    val keys = List(f("C001", 0), f("C001", 1), f("C002", 2), f("C002", 3), f("C002", 4))
    val bytes = keys.map(newSerde.toBytes)
    AvroSerde.binaryPrefixLength(classOf[CompoundKey]) should be(Some(17))
    val c001 = newSerde.prefix(classOf[CompoundKey], "UK", "C001")
    bytes.filter(key => ByteUtils.startsWith(key, c001)).size should be(2)
    val uk = newSerde.prefix(classOf[CompoundKey], "UK")
    bytes.filter(key => ByteUtils.startsWith(key, uk)).size should be(5)
    val c003 = newSerde.prefix(classOf[CompoundKey], "UK", "C003")
    bytes.filter(key => ByteUtils.startsWith(key, c003)).size should be(0)
  }

  "Long Compound Key " should "have the same binary prefix as Simple Key" in {
    AvroRecord.inferSchema[LongCompoundKey].toString should be("{\"type\":\"record\",\"name\":\"LongCompoundKey\",\"namespace\":\"io.amient.affinity.avro\",\"fields\":[{\"name\":\"version\",\"type\":{\"type\":\"fixed\",\"name\":\"version\",\"namespace\":\"\",\"size\":8,\"runtime\":\"long\"}},{\"name\":\"country\",\"type\":{\"type\":\"fixed\",\"name\":\"country\",\"namespace\":\"\",\"size\":2}},{\"name\":\"city\",\"type\":{\"type\":\"fixed\",\"name\":\"city\",\"namespace\":\"\",\"size\":4}},{\"name\":\"value\",\"type\":\"double\"}]}")
    val key = LongCompoundKey(100L, "UK", "C001", 99.9)
    val bytes = newSerde.toBytes(key)
    val prefix = newSerde.prefix(classOf[LongCompoundKey], new java.lang.Long(100L), "UK", "C001")
    assert(ByteUtils.startsWith(bytes, prefix))
    newSerde.fromBytes(bytes) should be(key)

  }

}
