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

import java.util.concurrent.TimeUnit
import java.util.{Properties, UUID}

import com.typesafe.config.ConfigFactory
import io.amient.affinity.avro.MemorySchemaRegistry
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.util.AffinityTestBase
import io.amient.affinity.kafka.{EmbeddedKafka, KafkaAvroSerializer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.util.Random

class ExampleBankSpec extends FlatSpec with AffinityTestBase with EmbeddedKafka with BeforeAndAfterAll with Matchers {
  override def numPartitions = 3

  val MemorySchemaRegistryId = "3453466"

  val testNodeDataDir = createTempDirectory
  val config = ConfigFactory.parseMap(Map(
    "kafka.bootstrap.servers" -> kafkaBootstrap,
    "affinity.node.data.dir" -> testNodeDataDir.toString,
    "affinity.avro.schema.registry.class" -> classOf[MemorySchemaRegistry].getName,
    "affinity.avro.schema.registry.id" -> MemorySchemaRegistryId
  ).asJava).withFallback(ConfigFactory.parseResources("example-bank.conf")).resolve()

  val node = new Node(configure(config, Some(zkConnect), Some(kafkaBootstrap)))

  val inputTopic = config.getString("affinity.node.gateway.stream.input-stream.kafka.topic")

  val producer = new KafkaProducer[Account, Transaction](new Properties() {
    import ProducerConfig._
    put(BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap)
    put(ACKS_CONFIG, "1")
    put(KEY_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
    put(VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
    put(CLIENT_ID_CONFIG, UUID.randomUUID.toString)
    put("schema.registry.class", classOf[MemorySchemaRegistry].getName)
    put("schema.registry.id", MemorySchemaRegistryId)
  })

  override def beforeAll(): Unit = {
    //produce test data for all tests
    val randomPartition = new Random()
    def produceTestTransaction(account: Account, t: Transaction): Long = {
      producer.send(new ProducerRecord(inputTopic, randomPartition.nextInt(numPartitions), t.timestamp, account, t)).get.offset
    }
    produceTestTransaction(Account("111030", 10233321), Transaction(1001, 99.9, timestamp = 1530000000000L)) //08am 26 June 2018
    produceTestTransaction(Account("111030", 10233321), Transaction(1001, 99.9, timestamp = 1530086400000L)) //08am 27 June 2018
    produceTestTransaction(Account("111030", 10233321), Transaction(1001, 99.9, timestamp = 1530172800000L)) //08am 28 June 2018
    node.start()
    node.awaitClusterReady()
    //TODO wait for all the last offset to be loaded (expose affinity test base function to do this and refactor the external state example as well
  }

  override def afterAll(): Unit = {
    try {
      node.shutdown()
      producer.close()
    } finally {
      deleteDirectory(testNodeDataDir)
    }
  }


  "ExampleWallet" should "store and index transactions by account prefix" in {
    //TODO make a http request to GetAccountTransactions for Account("111030", 10233321)
  }
}
