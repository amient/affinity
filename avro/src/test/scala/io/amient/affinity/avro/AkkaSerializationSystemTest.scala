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

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.Conf
import io.amient.affinity.avro.ConfluentSchemaRegistry.CfAvroConf
import io.amient.affinity.avro.record.AvroRecord
import io.amient.affinity.kafka.EmbeddedConfluentRegistry
import org.scalatest.FlatSpec


case class ExampleType(val id: Int) extends AvroRecord {
  override def hashCode(): Int = id.hashCode()
}

class AkkaSerializationSystemTest extends FlatSpec with EmbeddedConfluentRegistry {

  val config = ConfigFactory.defaultReference
    .withValue(Conf.Affi.Avro.Class.path, ConfigValueFactory.fromAnyRef(classOf[ConfluentSchemaRegistry].getName))
    .withValue(CfAvroConf(Conf.Affi.Avro).ConfluentSchemaRegistryUrl.path, ConfigValueFactory.fromAnyRef(registryUrl))


  assert(config.getString(CfAvroConf(Conf.Affi.Avro).ConfluentSchemaRegistryUrl.path) == registryUrl)

  override def numPartitions = 2

  "Confluent Schema Registry " should "be available via akka SerializationExtension" in {
    val system = ActorSystem.create("CfTest", config)
    try {
      val serialization = SerializationExtension(system)
      val serde = serialization.serializerFor(classOf[ExampleType])
      assert(serde.fromBinary(serde.toBinary(ExampleType(101))) == ExampleType(101))
    } finally {
      system.terminate()
    }
  }
}
