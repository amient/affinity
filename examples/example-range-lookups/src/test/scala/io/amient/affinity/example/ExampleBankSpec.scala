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

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.{Properties, UUID}

import akka.actor.{Actor, Props}
import com.typesafe.config.ConfigFactory
import io.amient.affinity.Conf
import io.amient.affinity.avro.MemorySchemaRegistry
import io.amient.affinity.avro.record.AvroSerde
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.storage.LogStorage
import io.amient.affinity.core.util.{AffinityTestBase, TimeRange}
import io.amient.affinity.kafka.{EmbeddedKafka, KafkaAvroSerializer}
import io.amient.affinity.spark.LogRDD
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
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

  val producerProps = new Properties() {

    import ProducerConfig._

    put(BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap)
    put(ACKS_CONFIG, "1")
    put(KEY_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
    put(VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
    put(CLIENT_ID_CONFIG, UUID.randomUUID.toString)
    put("schema.registry.class", classOf[MemorySchemaRegistry].getName)
    put("schema.registry.id", MemorySchemaRegistryId)
  }

  val txn1 = Transaction(1001, 99.9, timestamp = 1530000000000L) //08am 26 June 2018
  val txn2 = Transaction(1002, 99.9, timestamp = 1530000000000L) //08am 26 June 2018
  val txn3 = Transaction(1003, 99.9, timestamp = 1530086400000L) //08am 27 June 2018
  val txn4 = Transaction(1004, 99.9, timestamp = 1530090000000L) //09am 27 June 2018
  val txn5 = Transaction(1005, 99.9, timestamp = 1530172800000L) //08am 28 June 2018
  val txn6 = Transaction(1006, 99.9, timestamp = 1530172800000L) //08am 28 June 2018


  override def beforeAll(): Unit = {
    //produce test data for all tests
    val producer = new KafkaProducer[Account, Transaction](producerProps)
    try {
      val randomPartition = new Random()

      def produceTestTransaction(account: Account, t: Transaction): Unit = {
        producer.send(new ProducerRecord(inputTopic, randomPartition.nextInt(numPartitions), t.timestamp, account, t))
      }

      produceTestTransaction(Account("11-10-30", 10233321), txn1)
      produceTestTransaction(Account("33-55-10", 49772300), txn2)
      produceTestTransaction(Account("11-10-30", 10233321), txn3)
      produceTestTransaction(Account("11-10-30", 88885454), txn4)
      produceTestTransaction(Account("11-10-30", 10233321), txn5)
      produceTestTransaction(Account("11-10-30", 88885454), txn6)
      producer.flush()
    } finally {
      producer.close()
    }
    node.start()
    node.awaitClusterReady()

    //make sure that before starting the suite tests all the produced fixtures have been processed
    val latch = new CountDownLatch(6)
    node.system.eventStream.subscribe(
      node.system.actorOf(Props(new Actor {
        override def receive = {
          case _ => latch.countDown()
        }
      })), classOf[StoreTransaction])
    latch.await(10, TimeUnit.SECONDS)
  }

  override def afterAll(): Unit = {
    try {
      node.shutdown()
    } finally {
      deleteDirectory(testNodeDataDir)
    }
  }


  "ExampleWallet" should "should able to retrieve all transactions for the first account" in {
    node.get_json(node.http_get("/transactions/11-10-30/10233321")).getElements.asScala.size should be(3)
  }

  "ExampleWallet" should "should able to retrieve all transactions for the second account" in {
    node.get_json(node.http_get("/transactions/11-10-30/88885454")).getElements.asScala.size should be(2)
  }

  "ExampleWallet" should "should able to retrieve all transactions for the thrid account" in {
    node.get_json(node.http_get("/transactions/33-55-10/49772300")).getElements.asScala.size should be(1)
  }

  "ExampleWallet" should "should able to retrieve all transactions for the first branch" in {
    node.get_json(node.http_get("/transactions/11-10-30")).getElements.asScala.size should be(5)
  }

  "ExampleWallet" should "should able to retrieve all transactions for the second branch" in {
    node.get_json(node.http_get("/transactions/33-55-10")).getElements.asScala.size should be(1)
  }

  "ExampleWallet" should "should respond with empty transaction list for unknown branch" in {
    node.get_json(node.http_get("/transactions/xx-xx-xx")).getElements.asScala shouldBe empty
  }

  "ExampleWallet" should "should able to retrieve all transactions before a given date for the first branch" in {
    node.get_json(node.http_get("/transactions/11-10-30?before=2018-06-28")).getElements.asScala.size should be(3)
  }

  "ExampleWallet" should "be stored in kafka with event-time index" in {
    implicit val sc = new SparkContext(new SparkConf()
      .setMaster("local[1]")
      .set("spark.driver.host", "localhost")
      .setAppName("Example_Bank_Analytics")
      .set("spark.serializer", classOf[KryoSerializer].getName))

    implicit val conf = Conf(configure(config, Some(zkConnect), Some(kafkaBootstrap)))

    val avroConf = conf.Affi.Avro
    val storageConf = conf.Affi.Keyspace("default").State("transactions").Storage

    LogRDD(LogStorage.newInstance(storageConf), new TimeRange(txn4.timestamp, txn6.timestamp))
      .present[StorageKey, Transaction](AvroSerde.create(avroConf))
      .values.collect.sortBy(_.id) should be(Array(txn4, txn5, txn6))

    LogRDD(LogStorage.newInstance(storageConf), new TimeRange(txn3.timestamp, txn4.timestamp))
      .present[StorageKey, Transaction](AvroSerde.create(avroConf))
      .values.collect.sortBy(_.id) should be(Array(txn3, txn4))

    LogRDD(LogStorage.newInstance(storageConf), new TimeRange(txn1.timestamp, txn4.timestamp))
      .present[StorageKey, Transaction](AvroSerde.create(avroConf))
      .values.collect.sortBy(_.id) should be(Array(txn1, txn2, txn3, txn4))

  }

}
