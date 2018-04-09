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

package io.amient.affinity.kafka

import java.io.PrintStream
import java.util.Properties

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.avro.ConfluentSchemaRegistry.CfAvroConf
import io.amient.affinity.avro.ZookeeperSchemaRegistry.ZkAvroConf
import io.amient.affinity.avro.record.{AvroJsonConverter, AvroSerde}
import io.amient.affinity.avro.{ConfluentSchemaRegistry, ZookeeperSchemaRegistry}
import io.amient.affinity.core.util.EventTime
import kafka.common.MessageFormatter
import org.apache.avro.generic.GenericContainer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper


/**
  * See README of this module for Usage in kafka console consumer utility:
  *
  */
class AvroMessageFormatter extends MessageFormatter {

  private var serde: AvroSerde = null

  private var pretty = false
  private var printKey = false
  private var printValue = true
  private var printOffset = false
  private var printPartition = false
  private var printTimestamps = false
  private var printType = false

  override def init(props: Properties): Unit = {
    if (props.containsKey("pretty")) pretty = true
    if (props.containsKey("print.key")) printKey = true
    if (props.containsKey("print.offset")) printOffset = true
    if (props.containsKey("print.timestamp")) printTimestamps = true
    if (props.containsKey("print.partition")) printPartition = true
    if (props.containsKey("print.type")) printType = true
    if (props.containsKey("no.value")) printValue = false
    if (props.containsKey("schema.registry.url")) {
      serde = new ConfluentSchemaRegistry(ConfigFactory.empty
        .withValue(CfAvroConf.ConfluentSchemaRegistryUrl.path,
          ConfigValueFactory.fromAnyRef(props.getProperty("schema.registry.url"))))
    } else if (props.containsKey("schema.registry.zookeeper.connect")) {
      val config1 = ConfigFactory.empty
        .withValue(ZkAvroConf.ZooKeeper.Connect.path,
          ConfigValueFactory.fromAnyRef(props.getProperty("schema.registry.zookeeper.connect")))

      val config2 = if (!props.containsKey("schema.registry.zookeeper.root")) config1 else {
        config1.withValue(ZkAvroConf.ZkRoot.path,
          ConfigValueFactory.fromAnyRef(props.getProperty("schema.registry.zookeeper.root")))
      }
      serde = new ZookeeperSchemaRegistry(config2)
    } else {
      throw new IllegalArgumentException("Required --property schema.registry.url OR --property schema.zookeeper.connect")
    }
  }

  override def close(): Unit = {
    if (serde != null) {
      serde.close()
      serde = null
    }
  }

  val mapper = new ObjectMapper()

  override def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream): Unit = {

    if (printPartition) {
      output.print(consumerRecord.partition())
      output.print(": ")
    }
    if (printOffset) {
      output.print(consumerRecord.offset())
      output.print("\t")
    }
    if (printTimestamps) {
      output.print(EventTime.local(consumerRecord.timestamp()).toString)
      output.print("\t")
    }
    if (printKey) {
      val key: Any = serde.fromBytes(consumerRecord.key)
      val simpleJson: String = AvroJsonConverter.toJson(key)
      output.print(simpleJson)
      output.print("\t")
    }

    if (printValue) {
      val value: Any = serde.fromBytes(consumerRecord.value)
      if (printType) {
        value match {
          case container: GenericContainer => output.print(container.getSchema.getName)
          case other: Any => output.print(other.getClass.getSimpleName)
        }
        output.print(": ")
      }
      val simpleJson: String = AvroJsonConverter.toJson(value)
      if (pretty) {
        val json: JsonNode = mapper.readTree(simpleJson)
        output.print(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json))
      } else {
        output.print(simpleJson)
      }
    }

    output.println()
  }
}
