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
import io.amient.affinity.avro.{AvroJsonConverter, AvroSerde}
import io.amient.affinity.avro.schema.{CfAvroSchemaRegistry, ZkAvroSchemaRegistry}
import kafka.common.MessageFormatter
import org.apache.kafka.clients.consumer.ConsumerRecord


/**
  * Usage in kafka console consumer utility:
  *
  * kafka-console-consumer.sh \
  *   --formatter io.amient.affinity.kafka.AvroMessageFormatter \
  *   --property schema.zookeeper.connect=localhost:2181 OR --property schema.registry.url=http://localhost:8081 \
  *   --zookeeper ...\
  *   --topic ... \
  */
class AvroMessageFormatter extends MessageFormatter {

  private var serde: AvroSerde = null

  override def init(props: Properties): Unit = {
    if (props.containsKey("schema.registry.url")) {
      serde = new CfAvroSchemaRegistry(ConfigFactory.defaultReference()
        .withValue(CfAvroSchemaRegistry.CONFIG_CF_REGISTRY_URL_BASE,
          ConfigValueFactory.fromAnyRef(props.getProperty("schema.registry.url"))))
    } else if (props.containsKey("schema.zookeeper.connect")) {
      serde = new ZkAvroSchemaRegistry(ConfigFactory.defaultReference()
        .withValue(ZkAvroSchemaRegistry.CONFIG_ZOOKEEPER_CONNECT,
          ConfigValueFactory.fromAnyRef(props.getProperty("schema.zookeeper.connect"))))
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

  override def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream): Unit = {
    val x: Any = serde.fromBytes(consumerRecord.value())
    val schema = serde.getSchema(x)
    output.println(AvroJsonConverter.toJson(x, schema))
  }
}
