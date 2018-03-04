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

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.amient.affinity.Conf
import io.amient.affinity.avro.ZookeeperSchemaRegistry
import io.amient.affinity.avro.ZookeeperSchemaRegistry.ZkAvroConf
import io.amient.affinity.avro.record.AvroRecord
import io.amient.affinity.avro.record.AvroSerde.AvroConf
import io.amient.affinity.core.storage.State
import io.amient.affinity.core.util.{ByteUtils, AffinityTestBase}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.scalatest.{FlatSpec, Matchers}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object UUID {
  def apply(uuid: java.util.UUID): UUID = apply(ByteUtils.uuid(uuid))

  def random: UUID = apply(java.util.UUID.randomUUID)
}

case class UUID(val data: Array[Byte]) extends AvroRecord {
  def javaUUID: java.util.UUID = ByteUtils.uuid(data.array)
}

case class KEY(id: Int) extends AvroRecord {
  override def hashCode(): Int = id.hashCode()
}

case class TestRecord(key: KEY, uuid: UUID, ts: Long = 0L, text: String = "") extends AvroRecord {
  override def hashCode(): Int = key.hashCode()
}

class KafkaStorageSpec extends FlatSpec with AffinityTestBase with EmbeddedKafka with Matchers {

  val specTimeout = 15 seconds

  override def numPartitions: Int = 2

  private val log = LoggerFactory.getLogger(classOf[KafkaStorageSpec])

  val config = configure(ConfigFactory.load("kafkastoragetest")
    .withValue(Conf.Affi.Avro.Class.path, ConfigValueFactory.fromAnyRef(classOf[ZookeeperSchemaRegistry].getName))
    , Some(zkConnect), Some(kafkaBootstrap))

  val system = ActorSystem.create("KafkaEcoSystem", config)

  import system.dispatcher

  override def beforeAll: Unit = try {
    SerializationExtension(system)
  } finally {
    super.beforeAll()
  }

  override def afterAll(): Unit = try {
    system.terminate()
  } finally {
    super.afterAll()
  }

  private def createStateStoreForPartition(keyspace: String, partition: Int, store: String): State[Int, TestRecord] = {
    val conf = Conf(config)
    val stateConf = conf.Affi.Keyspace(keyspace).State(store)
    val state = State.create[Int, TestRecord](store, partition, stateConf, conf.Affi.Keyspace(keyspace).NumPartitions(), system)
    state.boot()
    state
  }

  behavior of "KafkaStorage"

  //FIXME #155
  it should "survive failing writes" ignore {
    config.getString(Conf.Affi.Avro.Class.path) should be (classOf[ZookeeperSchemaRegistry].getName)
    system.settings.config.getString(Conf.Affi.Avro.Class.path) should be (classOf[ZookeeperSchemaRegistry].getName)
    val stateStoreName = "failure-test"
    val topic = KafkaStorage.StateConf(Conf(config).Affi.Keyspace("keyspace1").State(stateStoreName)).Storage.Topic()
    val state = createStateStoreForPartition("keyspace1", partition = 0, stateStoreName)
    runTestWithState(state, topic, 100)
  }

  it should "fast forward to a min.timestamp on bootstrap if bigger than now - ttl" ignore {
    //FIXME add test for min.timestamp in storage bootstrap
  }
  it should "fast forward to a now - ttl if min.timestamp provided" ignore {
    //FIXME add test for state.ttl in storage bootstrap
  }

  it should "stop consuming after boot when state.external=false and " ignore {
    //FIXME add stroage test for state.external=true
  }
  it should "never enter boot() when state.external=true" ignore {
    createStateStoreForPartition("keyspace1", partition = 0, "external-test")
    //FIXME add stroage test for state.external=true
  }

  behavior of "KafkaDeserializer"

  it should "be able to work with ZkAvroSchemaRegistry" in {

    config.getString(Conf.Affi.Avro.Class.path) should be (classOf[ZookeeperSchemaRegistry].getName)
    system.settings.config.getString(Conf.Affi.Avro.Class.path) should be (classOf[ZookeeperSchemaRegistry].getName)

    val stateStoreName = "throughput-test"
    val topic = KafkaStorage.StateConf(Conf(config).Affi.Keyspace("keyspace1").State(stateStoreName)).Storage.Topic()
    val state = createStateStoreForPartition("keyspace1", partition = 0, stateStoreName)
    runTestWithState(state, topic, 10)
  }

  private def runTestWithState(state: State[Int, TestRecord], topic: String, numRecords: Int) {
    val numWrites = new AtomicInteger(numRecords)
    val numToWrite = numWrites.get
    val l = System.currentTimeMillis()
    val updates = Future.sequence(for (i <- (1 to numToWrite)) yield {
      state.replace(i, TestRecord(KEY(i), UUID.random, System.currentTimeMillis(), s"test value $i")) transform(
        (s) => s, (e: Throwable) => {
        numWrites.decrementAndGet()
        e
      })
    })
    Await.ready(updates, specTimeout)
    numWrites.get should be >= 0
    log.info(s"written ${numWrites.get} records of state data in ${System.currentTimeMillis() - l} ms")
    state.numKeys should equal(numWrites.get)

    val consumerProps = Map(
      "bootstrap.servers" -> kafkaBootstrap,
      "group.id" -> "group2",
      "auto.offset.reset" -> "earliest",
      "max.poll.records" -> 1000,
      "key.deserializer" -> classOf[KafkaAvroDeserializer].getName,
      "value.deserializer" -> classOf[KafkaAvroDeserializer].getName,
      new AvroConf().Class.path -> classOf[ZookeeperSchemaRegistry].getName,
      new ZkAvroConf().ZooKeeper.Connect.path -> zkConnect
    )

    val consumer = new KafkaConsumer[Int, TestRecord](consumerProps.mapValues(_.toString.asInstanceOf[AnyRef]))

    consumer.subscribe(List(topic))
    try {

      var read = 0
      val numReads = numWrites.get
      while (read < numReads) {
        val records = consumer.poll(specTimeout.toMillis)
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

}
