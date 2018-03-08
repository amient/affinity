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

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.config.ConfigFactory
import io.amient.affinity.Conf
import io.amient.affinity.avro.ConfluentSchemaRegistry
import io.amient.affinity.avro.ConfluentSchemaRegistry.CfAvroConf
import io.amient.affinity.avro.record.AvroRecord
import io.amient.affinity.avro.record.AvroSerde.AvroConf
import io.amient.affinity.core.serde.Serde
import io.amient.affinity.core.storage.{MemStoreSimpleMap, State}
import io.amient.affinity.core.util.ByteUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.{FlatSpec, Matchers}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

object UUID {
  def apply(uuid: java.util.UUID): UUID = apply(ByteUtils.uuid(uuid))

  def random: UUID = apply(java.util.UUID.randomUUID)
}

case class UUID(val data: Array[Byte]) extends AvroRecord {
  def javaUUID: java.util.UUID = ByteUtils.uuid(data)
}

case class KEY(id: Int) extends AvroRecord {
  override def hashCode(): Int = id.hashCode()
}

case class Test(key: KEY, uuid: UUID, ts: Long = 0L, text: String = "") extends AvroRecord {
  override def hashCode(): Int = key.hashCode()
}

class ConfluentEcoSystemSpec extends FlatSpec with EmbeddedKafka with EmbeddedConfluentRegistry with Matchers {

  override def numPartitions = 2

  private val log = LoggerFactory.getLogger(classOf[ConfluentEcoSystemSpec])

  val config = ConfigFactory.parseMap(Map(
      Conf.Affi.Avro.Class.path -> classOf[ConfluentSchemaRegistry].getName,
      CfAvroConf(Conf.Affi.Avro).ConfluentSchemaRegistryUrl.path -> registryUrl))

  "AvroRecords registered with Affinity" should "be visible to the Confluent Registry Client" in {
    val topic = "visibility-test"
    val state = createStateStoreForPartition(topic, 0)
    state.insert(1, Test(KEY(1), UUID.random, System.currentTimeMillis(), s"test value 1"))
    val testSchemaId = registryClient.getLatestSchemaMetadata(classOf[Test].getName).getId
    registryClient.getByID(testSchemaId) should equal(AvroRecord.inferSchema(classOf[Test]))
  }

  "Confluent KafkaAvroDeserializer" should "read Affinity AvroRecords as IndexedRecords (high throughput scenario)" in {
    testExternalKafkaConsumer("throughput-test")
  }

  "Confluent KafkaAvroDeserializer" should "read Affinity AvroRecords as IndexedRecords (failing writes scenario)" in {
    testExternalKafkaConsumer("failure-test")
  }

  private def testExternalKafkaConsumer(topic: String) {
    val state = createStateStoreForPartition(topic, 0)
    val numWrites = new AtomicInteger(5000)
    val numToWrite = numWrites.get
    val l = System.currentTimeMillis()
    state.boot()
    state.numKeys should be(0)
    val updates = for (i <- (1 to numToWrite)) yield {
      state.replace(i, Test(KEY(i), UUID.random, System.currentTimeMillis(), s"test value $i")) transform(
        (s) => s
        , (e: Throwable) => {
        numWrites.decrementAndGet()
        e
      })
    }

    updates.size should be(numToWrite)
    Await.result(Future.sequence(updates), 10 seconds)
    val spentMs = System.currentTimeMillis() - l
    log.info(s"written ${numWrites.get} records of state data in ${spentMs} ms at ${numWrites.get * 1000 / spentMs} tps")
    state.numKeys should equal(numWrites.get)
    state.iterator.size should equal(state.numKeys)

    val consumerProps = Map(
      "bootstrap.servers" -> kafkaBootstrap,
      "group.id" -> "group2",
      "auto.offset.reset" -> "earliest",
      "max.poll.records" -> 1000,
      "key.deserializer" -> classOf[KafkaAvroDeserializer].getName,
      "value.deserializer" -> classOf[KafkaAvroDeserializer].getName,
      AvroConf.Class.path -> classOf[ConfluentSchemaRegistry].getName,
      CfAvroConf.ConfluentSchemaRegistryUrl.path -> registryUrl
    )

    println(consumerProps)

    val consumer = new KafkaConsumer[Int, Test](consumerProps.mapValues(_.toString.asInstanceOf[AnyRef]))

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

  private def createStateStoreForPartition(topic: String, partition: Int) = {
    val stateConf = State.StateConf(ConfigFactory.parseMap(Map(
      State.StateConf.Storage.Class.path -> classOf[KafkaLogStorage].getName,
      State.StateConf.MemStore.Class.path -> classOf[MemStoreSimpleMap].getName,
      KafkaStorage.StateConf.Storage.Topic.path -> topic,
      KafkaStorage.StateConf.Storage.BootstrapServers.path -> kafkaBootstrap,
      KafkaStorage.StateConf.Storage.Producer.path -> Map().asJava,
      KafkaStorage.StateConf.Storage.Consumer.path -> Map().asJava
    )))
    val keySerde = Serde.of[Int](config)
    val valueSerde = Serde.of[Test](config)
    val kvstore = new MemStoreSimpleMap(stateConf)
    State.create(topic, partition, stateConf, numPartitions, kvstore, keySerde, valueSerde)
  }

  "Confluent KafkaAvroSerializer" should "be intercepted and given affinity subject" in {
    val producerProps = Map(
      "bootstrap.servers" -> kafkaBootstrap,
      "acks" -> "all",
      "linger.ms" -> 20,
      "batch.size" -> 20,
      "key.serializer" -> "io.confluent.kafka.serializers.KafkaAvroSerializer",
      "value.serializer" -> "io.confluent.kafka.serializers.KafkaAvroSerializer",
      "schema.registry.url" -> registryUrl
    )
    val topic = "test"
    val numWrites = new AtomicInteger(1000)
    val producer = new KafkaProducer[Int, Test](producerProps.mapValues(_.toString.asInstanceOf[AnyRef]))
    try {
      val numToWrite = numWrites.get
      val l = System.currentTimeMillis()
      val updates = Future.sequence(for (i <- (1 to numToWrite)) yield {
        val record = Test(KEY(i), UUID.random, System.currentTimeMillis(), s"test value $i")
        val f = producer.send(new ProducerRecord[Int, Test](topic, i, record))
        Future(f.get) transform(
          (s) => s, (e: Throwable) => {
          numWrites.decrementAndGet()
          e
        })
      })
      Await.ready(updates, 10 seconds)
      log.info(s"produced ${numWrites.get} records of state data in ${System.currentTimeMillis() - l} ms")

    } finally {
      producer.close()
    }
    //now bootstrap the state
    val state0 = createStateStoreForPartition(topic, 0)
    val state1 = createStateStoreForPartition(topic, 1)
    state0.boot()
    state1.boot()
//    //TODO #75 test that kafka default partitioner + confluent avro serde partitions as expected
    (state0.numKeys + state1.numKeys) should equal(numWrites.get)
  }

}


