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

package io.amient.affinity.systemtests.confluent

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.avro.AvroSerde.AvroConf
import io.amient.affinity.avro.schema.CfAvroSchemaRegistry
import io.amient.affinity.avro.schema.CfAvroSchemaRegistry.CfAvroConf
import io.amient.affinity.avro.{AvroRecord, AvroSerde}
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.storage.State
import io.amient.affinity.core.storage.kafka.KafkaStorage.KafkaStorageConf
import io.amient.affinity.core.util.SystemTestBase
import io.amient.affinity.kafka.{EmbeddedKafka, KafkaAvroDeserializer, KafkaObjectHashPartitioner}
import io.amient.affinity.systemtests.{KEY, TestRecord, UUID}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps


class ConfluentEcoSystemTest extends FlatSpec with SystemTestBase with EmbeddedKafka with EmbeddedCfRegistry with Matchers {

  override def numPartitions = 2

  val config = configure(
    ConfigFactory.load("systemtests")
      .withValue(CfAvroSchemaRegistry.Conf.Avro.ConfluentSchemaRegistryUrl.path, ConfigValueFactory.fromAnyRef(registryUrl))
      .withValue(AvroSerde.Conf.Avro.Class.path, ConfigValueFactory.fromAnyRef(classOf[CfAvroSchemaRegistry].getName))
    , Some(zkConnect), Some(kafkaBootstrap))

  val system = ActorSystem.create("ConfluentEcoSystem", config)

  import system.dispatcher

  override def beforeAll: Unit = {
    SerializationExtension(system)
  }

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  "AvroRecords registered with Affinity" should "be visible to the Confluent Registry Client" in {
    val stateStoreName = "visibility-test"
    val state = createStateStoreForPartition(stateStoreName)(0)
    state.insert(1, TestRecord(KEY(1), UUID.random, System.currentTimeMillis(), s"test value 1"))
    val testRecordSchemaId = registryClient.getLatestSchemaMetadata(classOf[TestRecord].getName).getId
    registryClient.getByID(testRecordSchemaId) should equal(AvroRecord.inferSchema(classOf[TestRecord]))
  }

  "Confluent KafkaAvroDeserializer" should "read Affinity AvroRecords as IndexedRecords (high throughput scenario)" in {
    testExternalKafkaConsumer("throughput-test")
  }

  "Confluent KafkaAvroDeserializer" should "read Affinity AvroRecords as IndexedRecords (failing writes scenario)" in {
    testExternalKafkaConsumer("failure-test")
  }

  private def testExternalKafkaConsumer(stateStoreName: String) {
    val topic = KafkaStorageConf(Node.Conf(config).Affi.State(stateStoreName).Storage).Topic()
    val state = createStateStoreForPartition(stateStoreName)(0)
    val numWrites = new AtomicInteger(5000)
    val numToWrite = numWrites.get
    val l = System.currentTimeMillis()
    state.numKeys should be(0)
    val updates = for (i <- (1 to numToWrite)) yield {
      state.replace(i, TestRecord(KEY(i), UUID.random, System.currentTimeMillis(), s"test value $i")) transform(
        (s) => s
        , (e: Throwable) => {
        numWrites.decrementAndGet()
        e
      })
    }
    updates.size should be(numToWrite)
    Await.result(Future.sequence(updates), 10 seconds)
    val spentMs = System.currentTimeMillis() - l
    println(s"written ${numWrites.get} records of state data in ${spentMs} ms at ${numWrites.get * 1000 / spentMs} tps")
    state.numKeys should equal(numWrites.get)
    state.iterator.size should equal(state.numKeys)

    val consumerProps = Map(
      "bootstrap.servers" -> kafkaBootstrap,
      "group.id" -> "group2",
      "auto.offset.reset" -> "earliest",
      "max.poll.records" -> 1000,
      "key.deserializer" -> classOf[KafkaAvroDeserializer].getName,
      "value.deserializer" -> classOf[KafkaAvroDeserializer].getName,
      new AvroConf().Class.path -> classOf[CfAvroSchemaRegistry].getName,
      new CfAvroConf().ConfluentSchemaRegistryUrl.path -> registryUrl
    )

    val consumer = new KafkaConsumer[Int, TestRecord](consumerProps.mapValues(_.toString.asInstanceOf[AnyRef]))

    consumer.subscribe(List(topic))
    try {

      var read = 0
      val numReads = numWrites.get
      while (read < numReads) {
        val records = consumer.poll(10000)
        if (records.isEmpty) throw new Exception("Consumer poll timeout")
        for (record <- records) {
          read += 1
          record.value.key.id should equal(record.key)
          record.value.text should equal(s"test value ${record.key}")
        }
      }
    } finally {
      consumer.close()
    }

  }

  private def createStateStoreForPartition(name: String)(implicit partition: Int) = {
    new State[Int, TestRecord](name, system)
  }

  "Confluent KafkaAvroSerializer" should "be intercepted and given affinity subject" in {
    val topic = KafkaStorageConf(Node.Conf(config).Affi.State("consistency-test").Storage).Topic()
    val producerProps = Map(
      "bootstrap.servers" -> kafkaBootstrap,
      "acks" -> "all",
      "linger.ms" -> 20,
      "batch.size" -> 20,
      "partitioner.class" -> classOf[KafkaObjectHashPartitioner].getName,
      "key.serializer" -> "io.confluent.kafka.serializers.KafkaAvroSerializer",
      "value.serializer" -> "io.confluent.kafka.serializers.KafkaAvroSerializer",
      "schema.registry.url" -> registryUrl
    )
    val numWrites = new AtomicInteger(1000)
    val producer = new KafkaProducer[Int, TestRecord](producerProps.mapValues(_.toString.asInstanceOf[AnyRef]))
    try {
      val numToWrite = numWrites.get
      val l = System.currentTimeMillis()
      val updates = Future.sequence(for (i <- (1 to numToWrite)) yield {
        val record = TestRecord(KEY(i), UUID.random, System.currentTimeMillis(), s"test value $i")
        val f = producer.send(new ProducerRecord[Int, TestRecord](topic, i, record))
        Future(f.get) transform(
          (s) => s, (e: Throwable) => {
          numWrites.decrementAndGet()
          e
        })
      })
      Await.ready(updates, 10 seconds)
      println(s"produced ${numWrites.get} records of state data in ${System.currentTimeMillis() - l} ms")

    } finally {
      producer.close()
    }
    //now bootstrap the state
    val state0 = createStateStoreForPartition(topic)(0)
    val state1 = createStateStoreForPartition(topic)(1)
    state0.storage.init()
    state1.storage.init()
    state0.storage.boot()
    state1.storage.boot()
    (state0.numKeys + state1.numKeys) should equal(numWrites.get)
  }

}


