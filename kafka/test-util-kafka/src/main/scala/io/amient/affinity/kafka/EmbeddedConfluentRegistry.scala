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

import java.nio.channels.ServerSocketChannel
import java.util.Properties

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.rest.{SchemaRegistryConfig, SchemaRegistryRestApplication}
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.slf4j.LoggerFactory

trait EmbeddedConfluentRegistry extends EmbeddedKafka with BeforeAndAfterAll {

  self: Suite =>

  private val log = LoggerFactory.getLogger(classOf[EmbeddedConfluentRegistry])

  private val registryConfig: SchemaRegistryConfig = new SchemaRegistryConfig(new Properties() {
    put("listeners", s"http://127.0.0.1:0")
    put("kafkastore.connection.url", zkConnect)
    put("avro.compatibility.level", "full")
    put("kafkastore.topic", "_schemas")
    put("debug", "true")
  })
  private val app = new SchemaRegistryRestApplication(registryConfig)
  private val registry = app.createServer
  registry.start()

  val registryUrl = s"http://127.0.0.1:" + registry.getConnectors.head.getTransport.asInstanceOf[ServerSocketChannel].socket.getLocalPort
  log.info("Confluent schema registry listening at: " + registryUrl)
  val registryClient = new CachedSchemaRegistryClient(registryUrl, 20)

  abstract override def afterAll() {
    registry.stop()
    super.afterAll()
  }

}
