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
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import io.amient.affinity.core.serde.avro.AvroRecord
import io.amient.affinity.core.storage.State
import io.amient.affinity.core.storage.kafka.KafkaStorage
import io.amient.affinity.kafka.consumer.AffinityKafkaConsumer
import io.amient.affinity.kafka.producer.AffinityKafkaProducer
import io.amient.affinity.testutil.SystemTestBaseWithConfluentRegistry
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class ConfluentEcoSystemTest extends FlatSpec with SystemTestBaseWithConfluentRegistry with Matchers {


  val config = configure {
    ConfigFactory.load("systemtests")
      .withValue("akka.actor.serializers.avro", ConfigValueFactory.fromAnyRef(classOf[TestAvroRegistry].getName))
  }

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
    val testRecordSchemaId = registryClient.getLatestSchemaMetadata(classOf[TestRecord].getName).getId
    registryClient.getByID(testRecordSchemaId) should equal(AvroRecord.inferSchema(classOf[TestRecord]))
    val uuidSchemaId = registryClient.getLatestSchemaMetadata(classOf[UUID].getName).getId
    registryClient.getByID(uuidSchemaId) should equal(AvroRecord.inferSchema(classOf[UUID]))
    val intSchemaId = registryClient.getLatestSchemaMetadata(classOf[Int].getName).getId
    registryClient.getByID(intSchemaId) should equal(AvroRecord.inferSchema(classOf[Int]))
  }

  "Confluent KafkaAvroDeserializer" should "read Affinity AvroRecords as IndexedRecords (high throughput scenario)" in {
    testExternalKafkaConsumer("throughput-test")
  }

  "Confluent KafkaAvroDeserializer" should "read Affinity AvroRecords as IndexedRecords (failing writes scenario)" in {
    testExternalKafkaConsumer("failure-test")
  }

  private def testExternalKafkaConsumer(stateStoreName: String) {
    val stateStoreConfig = config.getConfig(State.CONFIG_STATE_STORE(stateStoreName))
    val topic = stateStoreConfig.getString(KafkaStorage.CONFIG_KAFKA_TOPIC)
    val state = createStateStoreForPartition(stateStoreName, stateStoreConfig)(0)
    val numWrites = new AtomicInteger(5000)
    val numToWrite = numWrites.get
    val l = System.currentTimeMillis()
    val updates = Future.sequence(for (i <- (1 to numToWrite)) yield {
      state.update(i, TestRecord(KEY(i), UUID.random, System.currentTimeMillis(), s"test value $i")) transform(
        (s) => s, (e: Throwable) => {
        numWrites.decrementAndGet()
        e
      })
    })
    Await.ready(updates, 10 seconds)
    println(s"written ${numWrites.get} records of state data in ${System.currentTimeMillis() - l} ms")
    state.size should equal(numWrites.get)

    val consumerProps = Map(
      "bootstrap.servers" -> kafkaBootstrap,
      "group.id" -> "group2",
      "auto.offset.reset" -> "earliest",
      "max.poll.records" -> 1000,
      "schema.registry.url" -> registryUrl
    )

    val consumer = new AffinityKafkaConsumer[Int, TestRecord](consumerProps)

    consumer.subscribe(List(topic).asJava)
    try {

      var read = 0
      val numReads = numWrites.get
      while (read < numReads) {
        val records = consumer.poll(1000)
        if (records.isEmpty) throw new Exception("Consumer poll timeout")
        for (record <- records.asScala) {
          read += 1
          record.value.key.id should equal(record.key)
          record.value.text should equal(s"test value ${record.key}")
        }
      }
    } finally {
      consumer.close()
    }

  }

  private def createStateStoreForPartition(name: String, stateStoreConfig: Config)(implicit partition: Int) = {
    new State[Int, TestRecord](name, system, stateStoreConfig)
  }

  "Confluent KafkaAvroSerializer" should "be intercepted and given affinity subject" in {

    val stateStoreConfig = config.getConfig(State.CONFIG_STATE_STORE("consistency-test"))
    val topic = stateStoreConfig.getString(KafkaStorage.CONFIG_KAFKA_TOPIC)

    val producerProps = Map(
      "bootstrap.servers" -> kafkaBootstrap,
      "acks" -> "all",
      "linger.ms" -> 20,
      "batch.size" -> 20,
      "schema.registry.url" -> registryUrl
    )
    val numWrites = new AtomicInteger(1000)
    val producer = new AffinityKafkaProducer[Int, TestRecord](producerProps)
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
    val state0 = createStateStoreForPartition(topic, stateStoreConfig)(0)
    val state1 = createStateStoreForPartition(topic, stateStoreConfig)(1)
    state0.storage.init()
    state1.storage.init()
    state0.storage.boot()
    state1.storage.boot()
    (state0.size + state1.size) should equal(numWrites.get)
  }

}


