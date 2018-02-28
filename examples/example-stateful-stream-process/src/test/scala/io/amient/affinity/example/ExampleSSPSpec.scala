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

package io.amient.affinity.example

import java.util.{Properties, UUID}

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions, ConfigValueFactory}
import io.amient.affinity.avro.MemorySchemaRegistry
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.util.AffinityTestBase
import org.scalatest.BeforeAndAfterAll
import io.amient.affinity.kafka.EmbeddedKafka
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.{LongDeserializer, LongSerializer, StringDeserializer, StringSerializer}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class ExampleSSPSpec extends FlatSpec with AffinityTestBase with EmbeddedKafka with BeforeAndAfterAll with Matchers {
  override def numPartitions = 1

  val MemorySchemaRegistryId = "123423542"

  val config = ConfigFactory.parseMap(Map(
    "kafka.bootstrap.servers" -> kafkaBootstrap,
    "affinity.avro.schema.registry.class" -> classOf[MemorySchemaRegistry].getName,
    "affinity.avro.schema.registry.id" -> MemorySchemaRegistryId,
    "affinity.node.gateway.http.host" -> "0.0.0.0",
    "affinity.node.gateway.http.port" -> "0"
  ).asJava)
    .withFallback(ConfigFactory.parseResources("example-ssp.conf"))
    .resolve()
    .withFallback(ConfigFactory.defaultReference())



  val node = new Node(config)

  val inputTopic = config.getString("affinity.node.gateway.stream.input-stream.kafka.topic")

  val outputTopic = config.getString("affinity.node.gateway.stream.output-stream.kafka.topic")

  val producer = new KafkaProducer[String, Long](new Properties() {
    import ProducerConfig._
    put(BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap)
    put(ACKS_CONFIG, "1")
    put(KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    put(VALUE_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer])
    put(CLIENT_ID_CONFIG, UUID.randomUUID.toString)
  })

  val consumerConfig = new Properties() {
    import ConsumerConfig._
    put(BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap)
    put(KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    put(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[LongDeserializer].getName)
    put(GROUP_ID_CONFIG, UUID.randomUUID.toString)
    put(AUTO_OFFSET_RESET_CONFIG, "earliest")
    put("schema.registry.class", classOf[MemorySchemaRegistry].getName)
    put("schema.registry.id", MemorySchemaRegistryId)
  }


  override def beforeAll(): Unit = {
    node.start()
    node.awaitClusterReady()
    println("UP!")
  }

  override def afterAll(): Unit = {
    node.shutdown()
    producer.close()
    println("DOWN!")
    Thread.sleep(5000)
  }

  "ExampleSSP" should "work" in {

  }
}
