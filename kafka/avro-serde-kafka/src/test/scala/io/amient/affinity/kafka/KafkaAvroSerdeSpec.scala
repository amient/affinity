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

package io.amient.affinity.kafka

import java.util

import io.amient.affinity.avro.{ConfluentSchemaRegistry, MemorySchemaRegistry}
import org.scalatest.{FlatSpec, Matchers}

class KafkaAvroSerdeSpec extends FlatSpec with Matchers {

  behavior of "KafkaAvroSerde for Kafka Streams"

  it should "work with a SchemaRegistry"  in {
    val serde = new SpecificKafkaAvroSerde[SimpleKey]
    serde.configure(new util.HashMap[String,String] {
      put("schema.registry.class", classOf[MemorySchemaRegistry].getName)
      put("schema.registry.id", "1")
    }, true)
    val value = SimpleKey(1)
    val x: Array[Byte] = serde.serializer.serialize("test", value)
    serde.deserializer.deserialize("test", x) should be(value)
  }

}
