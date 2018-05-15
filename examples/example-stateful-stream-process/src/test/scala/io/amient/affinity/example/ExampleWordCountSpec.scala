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

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.{Properties, UUID}

import com.typesafe.config.ConfigFactory
import io.amient.affinity.avro.MemorySchemaRegistry
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.util.AffinityTestBase
import io.amient.affinity.kafka.{EmbeddedKafka, KafkaAvroDeserializer}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.JavaConverters._

class ExampleWordCountSpec extends FlatSpec with AffinityTestBase with EmbeddedKafka with BeforeAndAfterAll with Matchers {
  override def numPartitions = 1

  val MemorySchemaRegistryId = "123423542"

  val config = ConfigFactory.parseMap(Map(
    "kafka.bootstrap.servers" -> kafkaBootstrap,
    "affinity.avro.schema.registry.class" -> classOf[MemorySchemaRegistry].getName,
    "affinity.avro.schema.registry.id" -> MemorySchemaRegistryId
  ).asJava).withFallback(ConfigFactory.parseResources("example-ssp.conf")).resolve()

  val inputTopic = config.getString("affinity.node.gateway.stream.input-stream.kafka.topic")

  val outputTopic = config.getString("affinity.node.gateway.stream.output-stream.kafka.topic")

  val producer = new KafkaProducer[Array[Byte], Array[Byte]](new Properties() {
    import ProducerConfig._
    put(BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap)
    put(ACKS_CONFIG, "1")
    put(KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    put(VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    put(CLIENT_ID_CONFIG, UUID.randomUUID.toString)
  })

  val consumerConfig = new Properties() {
    import ConsumerConfig._
    put(BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap)
    put(KEY_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getName)
    put(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getName)
    put(GROUP_ID_CONFIG, "test")
    put(AUTO_OFFSET_RESET_CONFIG, "earliest")
    put("schema.registry.class", classOf[MemorySchemaRegistry].getName)
    put("schema.registry.id", MemorySchemaRegistryId)
  }


  override def beforeAll(): Unit = try {

  } finally {
    super.beforeAll()
  }

  override def afterAll(): Unit = try {
    producer.close()
  } finally {
    super.afterAll()
  }

  "ExampleSSP" should "work" in {
    val consumer = new KafkaConsumer[String, Long](consumerConfig)
    consumer.subscribe(List(outputTopic).asJava)
    val outputQueue = new LinkedBlockingQueue[ConsumerRecord[String, Long]]()
    def poll(): (String, Long) = {
      if (outputQueue.isEmpty) outputQueue.addAll(consumer.poll(10000).records(outputTopic).asScala.toList.asJava)
      val record = outputQueue.poll(3000, TimeUnit.MILLISECONDS)
      if (record == null) null else (record.key, record.value)
    }

    try {
      val node = new Node(config)
      try {
        node.start()
        node.awaitClusterReady()
        producer.send(new ProducerRecord(inputTopic, null, "Hello".getBytes)).get()
        poll() should be(("Hello", 1))
        producer.send(new ProducerRecord(inputTopic, null, "Hello".getBytes)).get()
        poll() should be(("Hello", 2))
        producer.send(new ProducerRecord(inputTopic, null, "Hello".getBytes)).get()
        poll() should be(("Hello", 3))
        producer.send(new ProducerRecord(inputTopic, null, "World".getBytes)).get()
        poll() should be(("World", 1))
        producer.send(new ProducerRecord(inputTopic, null, "Hello World".getBytes)).get()
        Set(poll(), poll()) should be(Set(("Hello", 4), ("World", 2)))
      } finally {
        node.shutdown()
      }

      //restarting the node to test input stream resume behaviour")
      val node2 = new Node(config)
      try {
        node2.start()
        node2.awaitClusterReady()
        poll() should be(null)
      } finally {
        node2.shutdown()
      }
    } finally {
      consumer.close()
    }

  }
}
