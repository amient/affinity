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

import java.util.Properties

import com.typesafe.config.ConfigFactory
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.util.AffinityTestBase
import io.amient.affinity.kafka.EmbeddedKafka
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.{Millis, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.JavaConverters._

class ExampleExternalStateSpec extends FlatSpec with AffinityTestBase with EmbeddedKafka with Matchers with BeforeAndAfterAll
  with TimeLimitedTests {

  override def numPartitions = 2

  val config = configure(ConfigFactory.load("example-external-state"))

  val topic = config.getString("affinity.keyspace.external.state.news.storage.kafka.topic")

  val node = new Node(configure(config, Some(zkConnect), Some(kafkaBootstrap)))

  override def beforeAll: Unit = {
    val externalProducer = createKafkaAvroProducer[String, String]()
    try {
      externalProducer.send(new ProducerRecord(topic, "10:30", "the universe is expanding"))
      externalProducer.send(new ProducerRecord(topic, "11:00", "the universe is still expanding"))
      externalProducer.send(new ProducerRecord(topic, "11:30", "the universe briefly contracted but is expanding again"))
      externalProducer.flush()
    } finally {
      externalProducer.close()
    }
    //the external fixture is produced and the externalProducer is flushed() before the node is started
    node.start()
    node.awaitClusterReady()
    //at this point all stores have loaded everything available in the external topic so the test will be deterministic
  }

  override def afterAll: Unit = {
    node.shutdown()
  }

  behavior of "External State"

  val timeLimit = Span(5000, Millis) //it should be much faster but sometimes many tests are run at the same time

  it should "start automatically tailing state partitions on startup even when master" in {
    //we don't need an arbitrary sleep to ensure the tailing state catches up with the writes above
    //before we fetch the latest news because the watermark is built into the request to make the test fast and deterministic
    val response = node.get_text(node.http_get(s"/news/latest"))
    response should include("10:30\tthe universe is expanding")
    response should include("11:00\tthe universe is still expanding")
    response should include("11:30\tthe universe briefly contracted but is expanding again")

  }

  private def createKafkaAvroProducer[K, V]() = new KafkaProducer[K, V](new Properties {
    put("bootstrap.servers", kafkaBootstrap)
    put("acks", "1")
    put("key.serializer", "io.amient.affinity.kafka.KafkaAvroSerializer")
    put("value.serializer", "io.amient.affinity.kafka.KafkaAvroSerializer")
    //this simply adds all configs required by KafkaAvroSerializer
    config.getConfig("affinity.avro").entrySet().asScala.foreach { case (entry) =>
      put(entry.getKey, entry.getValue.unwrapped())
    }
  })


}
