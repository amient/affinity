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
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper


/**
  * Usage in kafka console consumer utility:
  *
  * kafka-console-consumer.sh \
  *   --formatter io.amient.affinity.kafka.AvroMessageFormatter \
  *  [--property schema.registry.zookeeper.connect=localhost:2181 \]
  *  [--property schema.registry.url=http://localhost:8081 \]
  *  [--property pretty \]
  *  [--property print.key \]
  *  [--property print.timestamp \]
  *   --zookeeper ...\
  *   --topic ... \
  */
class AvroMessageFormatter extends MessageFormatter {

  private var serde: AvroSerde = null

  private var pretty = false
  private var printKeys = false
  private var printTimestamps = false

  override def init(props: Properties): Unit = {
    if (props.containsKey("pretty")) pretty = true
    if (props.containsKey("print.key")) printKeys = true
    if (props.containsKey("print.timestamp")) printTimestamps = true
    if (props.containsKey("schema.registry.url")) {
      serde = new ConfluentSchemaRegistry(ConfigFactory.empty
        .withValue(new CfAvroConf().ConfluentSchemaRegistryUrl.path,
          ConfigValueFactory.fromAnyRef(props.getProperty("schema.registry.url"))))
    } else if (props.containsKey("schema.registry.zookeeper.connect")) {
      val config1 = ConfigFactory.empty
        .withValue(new ZkAvroConf().Connect.path,
          ConfigValueFactory.fromAnyRef(props.getProperty("schema.registry.zookeeper.connect")))

      val config2 = if (!props.containsKey("schema.registry.zookeeper.root")) config1 else {
        config1.withValue(new ZkAvroConf().Root.path,
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

    if (printTimestamps) {
      output.print(EventTime.local(consumerRecord.timestamp()).toString)
      output.print("\t")
    }
    if (printKeys) {
      val key: Any = serde.fromBytes(consumerRecord.key)
      val simpleJson: String = AvroJsonConverter.toJson(key)
      output.print(simpleJson)
      output.print("\t")
    }

    val value: Any = serde.fromBytes(consumerRecord.value)
    val simpleJson: String = AvroJsonConverter.toJson(value)
    if (pretty) {
      val json: JsonNode = mapper.readTree(simpleJson)
      output.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json))
    } else {
      output.println(simpleJson)
    }
  }
}
