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

package io.amient.affinity.testutil

import java.nio.channels.ServerSocketChannel

import com.typesafe.config.{Config, ConfigValueFactory}
import io.amient.affinity.core.serde.avro.schema.CfAvroSchemaRegistry
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.rest.{SchemaRegistryConfig, SchemaRegistryRestApplication}
import scala.collection.JavaConverters._

trait SystemTestBaseWithConfluentRegistry extends SystemTestBaseWithKafka {

  private val registryConfig: SchemaRegistryConfig = new SchemaRegistryConfig(new java.util.HashMap[Object, Object]() {
    put("listeners", s"http://0.0.0.0:0")
    put("kafkastore.connection.url", zkConnect)
    put("avro.compatibility.level", "full")
    put("kafkastore.topic", "_schemas")
    put("debug", "true")
  })
  private val app = new SchemaRegistryRestApplication(registryConfig)
  private val server = app.createServer
  server.start()
  val registryUrl = s"http://0.0.0.0:" + server.getConnectors.head.getTransport.asInstanceOf[ServerSocketChannel].socket.getLocalPort
  val registryClient = new CachedSchemaRegistryClient(registryUrl, 20)

  override def configure(config: Config) = super.configure(config)
    .withValue(CfAvroSchemaRegistry.CONFIG_CF_REGISTRY_URL_BASE, ConfigValueFactory.fromAnyRef(registryUrl))
    .withValue("akka.actor.serialization-bindings", ConfigValueFactory.fromMap(Map(
      "io.amient.affinity.core.serde.avro.AvroRecord" -> "avro"
    ).asJava))

  override def afterAll() {
    server.stop()
    super.afterAll()
  }
}
