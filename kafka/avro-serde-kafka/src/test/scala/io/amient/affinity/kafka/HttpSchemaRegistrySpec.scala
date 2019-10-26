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

import io.amient.affinity.avro.HttpSchemaRegistry
import io.amient.affinity.avro.HttpSchemaRegistry.HttpAvroConf
import io.amient.affinity.avro.record.AvroRecord
import io.amient.affinity.avro.record.AvroSerde.AvroConf
import org.apache.avro.Schema
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

object SimpleEnum extends Enumeration {
  type SimpleEnum = Value
  val A, B, C = Value
}

case class SimpleKey(val id: Int) extends AvroRecord {
  override def hashCode(): Int = id.hashCode()
}

case class SimpleRecord(val id: SimpleKey = SimpleKey(0), val side: SimpleEnum.Value = SimpleEnum.A, val seq: Seq[SimpleKey] = Seq()) extends AvroRecord{
  override def hashCode(): Int = id.hashCode()
}

case class CompositeRecord(
                   val items: Seq[SimpleRecord] = Seq(),
                   val index: Map[String, SimpleRecord] = Map(),
                   val setOfPrimitives: Set[Long] = Set() ) extends AvroRecord


class HttpSchemaRegistrySpec extends FlatSpec with Matchers with EmbeddedConfluentRegistry {

  override def numPartitions = 1

  behavior of "HttpSchemaRegistry"

  val serde = new HttpSchemaRegistry(HttpAvroConf(Map(
    HttpAvroConf(AvroConf).HttpSchemaRegistryUrl.path -> registryUrl
  ).asJava))

  serde.register[SimpleKey]
  serde.register[SimpleRecord]
  val v1schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Record\",\"namespace\":\"io.amient.affinity.kafka\",\"fields\":[{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"SimpleRecord\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"record\",\"name\":\"SimpleKey\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]},\"default\":{\"id\":0}},{\"name\":\"side\",\"type\":{\"type\":\"enum\",\"name\":\"SimpleEnum\",\"symbols\":[\"A\",\"B\",\"C\"]},\"default\":\"A\"},{\"name\":\"seq\",\"type\":{\"type\":\"array\",\"items\":\"SimpleKey\"},\"default\":[]}]}},\"default\":[]},{\"name\":\"removed\",\"type\":\"int\",\"default\":0}]}")
  serde.register[CompositeRecord](v1schema)

  it should "allow compatible version of previously registered schema" in {
    serde.register[CompositeRecord] should be(4)
  }

  it should "reject incompatible schema registration" in {

    val thrown = intercept[RuntimeException]{
      val v3schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Record\",\"namespace\":\"io.amient.affinity.kafka\",\"fields\":[{\"name\":\"data\",\"type\":\"string\"}]}")
      serde.register[CompositeRecord](v3schema)
    }
    thrown.getMessage should include("incompatible")

  }

  it should "register topic subject when fqn subject is already registered" in {
    val data = SimpleRecord()
    //fqn should be already registered
    serde.getRuntimeSchema(classOf[SimpleRecord].getName) should be((2, data.getSchema))
    //now simulate what KafkaAvroSerde would do
    val (schemaId, objSchema) = serde.from(data, "topic-simple")
    schemaId should be(2)
    objSchema should be(data.getSchema)
    //and check the additional subject was registered with the same schema
    serde.register("topic-simple", data.getSchema) should be(2)
  }
}
