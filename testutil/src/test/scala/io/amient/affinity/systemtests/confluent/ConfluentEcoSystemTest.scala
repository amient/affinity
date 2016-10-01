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

import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.core.serde.avro.AvroRecord
import io.amient.affinity.core.storage.State
import io.amient.affinity.core.storage.kafka.KafkaStorage
import io.amient.affinity.testutil.SystemTestBaseWithConfluentRegistry
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.IndexedRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
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
    test("throughput-test")
  }

  "Confluent KafkaAvroDeserializer" should "read Affinity AvroRecords as IndexedRecords (failing writes scenario)" in {
    test("failure-test")
  }

  private def test(stateStoreName: String) {
    implicit val partition = 0
    val stateStoreConfig = config.getConfig(State.CONFIG_STATE_STORE(stateStoreName))
    val state = new State[Int, TestRecord](system, stateStoreConfig)
    println(s"kafka available at zookeeper connection $zkConnect")
    val numWrites = new AtomicInteger(5000)
    val numToWrite = numWrites.get
    val l = System.currentTimeMillis()
    val updates = Future.sequence(for (i <- (1 to numToWrite)) yield {
      state.put(i, TestRecord(KEY(i), UUID.random, System.currentTimeMillis(), s"test value $i")) transform(
        (s) => s, (e: Throwable) => {
        numWrites.decrementAndGet(); e
      })
    })
    Await.ready(updates, 10 seconds)
    println(s"written ${numWrites.get} records of state data in ${System.currentTimeMillis() - l} ms")
    state.size should equal(numWrites.get)

    val props = new Properties()
    props.put("bootstrap.servers", kafkaBootstrap)
    props.put("group.id", "group2")
    props.put("auto.offset.reset", "earliest")
    props.put("max.poll.records", "1000")
    props.put("schema.registry.url", registryUrl)
    props.put("key.deserializer", classOf[KafkaAvroDeserializer].getName)
    props.put("value.deserializer", classOf[KafkaAvroDeserializer].getName)

    val consumer = new KafkaConsumer[Int, IndexedRecord](props)
    consumer.subscribe(List(stateStoreConfig.getString(KafkaStorage.CONFIG_KAFKA_TOPIC)).asJava)
    try {

      var read = 0
      val numReads = numWrites.get
      while (read < numReads) {
        val records = consumer.poll(1000)
        if (records.isEmpty) throw new Exception("Consumer poll timeout")
        for (record <- records.asScala) {
          read += 1
          //TODO #16 provide an efficient KafkaAvroSerde either as a wrapper or from scratch
          val key = KEY(record.value.get(0).asInstanceOf[IndexedRecord].get(0).asInstanceOf[Int])
          val uuid = UUID(record.value.get(1).asInstanceOf[IndexedRecord].get(0).asInstanceOf[ByteBuffer])
          val ts = record.value.get(2).asInstanceOf[Long]
          key.id should equal(record.key)
          AvroRecord.read[TestRecord](record.value) should equal {
            TestRecord(key, uuid, ts, s"test value ${key.id}")
          }
        }
      }
    } finally {
      consumer.close()
    }

  }

}


