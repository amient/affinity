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

package io.amient.affinity.testutil

import java.io.File
import java.util.Properties

import io.amient.affinity.core.ack._
import io.amient.affinity.core.serde.primitive.StringSerde
import io.amient.affinity.core.storage.MemStoreSimpleMap
import io.amient.affinity.core.storage.kafka.KafkaStorage
import io.amient.affinity.core.util.ZooKeeperClient
import kafka.cluster.Broker
import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.apache.kafka.common.protocol.SecurityProtocol

trait SystemTestBaseWithKafka extends SystemTestBase {

  private val embeddedKafkaPath = new File(testDir, "local-kafka-logs")
  private val kafkaConfig = new KafkaConfig(new Properties {
    {
      put("broker.id", "1")
      put("host.name", "localhost")
      put("port", "0")
      put("log.dir", embeddedKafkaPath.toString)
      put("num.partitions", "2")
      put("auto.create.topics.enable", "true")
      put("zookeeper.connect", zkConnect)
    }
  })
  private val kafka = new KafkaServerStartable(kafkaConfig)
  kafka.startup()

  val tmpZkClient = new ZooKeeperClient(zkConnect)
  val broker = Broker.createBroker(1, tmpZkClient.readData[String]("/brokers/ids/1"))
  val kafkaBootstrap = broker.getBrokerEndPoint(SecurityProtocol.PLAINTEXT).connectionString()
  tmpZkClient.close

  override def afterAll(): Unit = {
    try {
      kafka.shutdown()
    } catch {
      case e: IllegalStateException => //
    }
    super.afterAll()
  }

  class MyTestPartition(topic: String, rname: String) extends TestPartition(rname) {
    val data = storage {
      new KafkaStorage[String, String](kafkaBootstrap, topic, partition, classOf[StringSerde], classOf[StringSerde])
        with MemStoreSimpleMap[String, String]
    }

    override def handle: Receive = {
      case key: String => sender ! data.get(key)
      case (key: String, value: String) => ack(sender) {
        data.put(key, Some(value))
      }
    }
  }
}
