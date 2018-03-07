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

import io.amient.affinity.avro.record.AvroJsonConverter
import org.scalatest.{FlatSpec, Matchers}

class AvroJsonConverterSpec extends FlatSpec with Matchers {

  behavior of "AvroJsonConverter"

  it should "serialize to and from case class <-> avro <-> json with identical result to circe lib" in {
    val msg = AvroNamedRecords(SimpleKey(99), Some(SimpleKey(99)), None, List(SimpleKey(99), SimpleKey(100)), List(None, Some(SimpleKey(99)), None))
    val avroJson = AvroJsonConverter.toJson(msg, false)
    avroJson should be ("{\"e\":{\"id\":99},\"rn\":{\"id\":99},\"rs\":null,\"l\":[{\"id\":99},{\"id\":100}],\"lo\":[null,{\"id\":99},null]}")
    AvroJsonConverter.toAvro(avroJson, msg.getSchema()) should be (msg)
  }

  it should "handle primitives, strings and nulls" in {
    val msg = AvroPrmitives()
    val avroJson = AvroJsonConverter.toJson(msg)
    avroJson should be("{\"bn\":null,\"bs\":true,\"in\":null,\"is\":-2147483648,\"ln\":null,\"ls\":-9223372036854775808,\"fn\":null,\"fs\":-3.4028235E38,\"dn\":null,\"ds\":-1.7976931348623157E308,\"sn\":null,\"ss\":\"Hello\"}")
    AvroJsonConverter.toAvro(avroJson, msg.getSchema()) should be (msg)
  }

  it should "handle enums" in {
    val msg = AvroEnums(SimpleEnum.B, Some(SimpleEnum.B), None, List(SimpleEnum.A, SimpleEnum.B), List(None, Some(SimpleEnum.B)))
    val avroJson = AvroJsonConverter.toJson(msg)
    avroJson should be("{\"raw\":\"B\",\"on\":\"B\",\"sd\":null,\"l\":[\"A\",\"B\"],\"lo\":[null,\"B\"]}")
    AvroJsonConverter.toAvro(avroJson, msg.getSchema()) should be (msg)
  }

  it should "handle bytes" in {
    val msg = AvroBytes(Array[Byte](1,2,3), Some(Array[Byte]()), List(Array[Byte](1,2,3), Array[Byte](4)))
    val avroJson = AvroJsonConverter.toJson(msg)
    val avroMsg = AvroJsonConverter.toAvro(avroJson, msg.getSchema()).asInstanceOf[AvroBytes]
    avroMsg.raw should be (Array[Byte](1,2,3))
    avroMsg.optional.get should be (Array[Byte]())
    avroMsg.listed(0) should be(Array[Byte](1,2,3))
    avroMsg.listed(1) should be(Array[Byte](4))
  }

  it should "handle fixed field variants" in {
    val msg = LongCompoundKey(100L, "UK", "C001", 9.9)
    val avroJson = AvroJsonConverter.toJson(msg)
    println(avroJson)
    AvroJsonConverter.toAvro(avroJson, msg.getSchema()) should be (msg)
  }

}
