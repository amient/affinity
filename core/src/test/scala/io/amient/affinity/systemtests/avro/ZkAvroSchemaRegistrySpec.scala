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

package io.amient.affinity.systemtests.avro

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import io.amient.affinity.core.serde.avro._
import io.amient.affinity.core.serde.avro.schema.ZkAvroSchemaRegistry
import io.amient.affinity.core.util.ZooKeeperClient
import io.amient.affinity.testutil.SystemTestBaseWithZk
import org.apache.avro.Schema
import org.scalatest.{FlatSpec, Matchers}

class ZkAvroSchemaRegistrySpec extends FlatSpec with Matchers with SystemTestBaseWithZk {

  val v1schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Composite\",\"namespace\":\"io.amient.affinity.core.serde.avro\",\"fields\":[{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"Base\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"record\",\"name\":\"ID\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]},\"default\":{\"id\":0}},{\"name\":\"side\",\"type\":{\"type\":\"enum\",\"name\":\"Side\",\"symbols\":[\"LEFT\",\"RIGHT\"]},\"default\":\"LEFT\"},{\"name\":\"seq\",\"type\":{\"type\":\"array\",\"items\":\"ID\"},\"default\":[]}]}},\"default\":[]},{\"name\":\"removed\",\"type\":\"int\",\"default\":0}]}")
  val v3schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Composite\",\"namespace\":\"io.amient.affinity.core.serde.avro\",\"fields\":[{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"Base\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"record\",\"name\":\"ID\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]},\"default\":{\"id\":0}},{\"name\":\"side\",\"type\":{\"type\":\"enum\",\"name\":\"Side\",\"symbols\":[\"LEFT\",\"RIGHT\"]},\"default\":\"LEFT\"},{\"name\":\"seq\",\"type\":{\"type\":\"array\",\"items\":\"ID\"},\"default\":[]}]}},\"default\":[]},{\"name\":\"index\",\"type\":{\"type\":\"map\",\"values\":\"Base\"},\"default\":[]}]}")

  println("ZkAvroSchemaRegistrySpec zk " + zkConnect)
  val client = new ZooKeeperClient(zkConnect)
  val system = ActorSystem.create("TestActorSystem", configure())
  val serde = new ZkAvroSchemaRegistry(SerializationExtension(system).system)
  serde.register(classOf[ID])
  serde.register(classOf[Base])
  val backwardSchemaId = serde.register(classOf[Composite], v1schema)
  val currentSchemaId = serde.register(classOf[Composite])
  val forwardSchemaId = serde.register(classOf[Composite], v3schema)

  "ZkAvroRegistry" should "work in a backward-compatibility scenario" in {
    val oldValue = _V1_Composite(Seq(Base(ID(1), Side.LEFT)), 10)
    val oldBytes = AvroRecord.write(oldValue, v1schema, backwardSchemaId)
    val upgraded = serde.fromBinary(oldBytes)
    upgraded should be(Composite(Seq(Base(ID(1), Side.LEFT)), Map()))
  }

  "ZkAvroRegistry" should "work in a forward-compatibility scenario" in {
    val forwardValue = _V3_Composite(Seq(Base(ID(1), Side.LEFT)), Map("X" -> Base(ID(1), Side.LEFT)))
    val forwardBytes = AvroRecord.write(forwardValue, v3schema, forwardSchemaId)
    val downgraded = serde.fromBinary(forwardBytes)
    downgraded should be(Composite(Seq(Base(ID(1), Side.LEFT)), Map("X" -> Base(ID(1), Side.LEFT)), Set()))
  }

}
