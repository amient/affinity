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

import java.io.File
import java.util.Properties
import java.util.concurrent.TimeUnit

import kafka.cluster.Broker
import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.postfixOps

trait EmbeddedKafka extends EmbeddedZooKeeper with BeforeAndAfterAll {

  self: Suite =>

  private val log = LoggerFactory.getLogger(classOf[EmbeddedKafka])

  def numPartitions: Int

  private val embeddedKafkaPath = new File(testDir, "local-kafka-logs")
  private val kafkaConfig = new KafkaConfig(new Properties {
    put("broker.id", "1")
    put("host.name", "localhost")
    put("port", "0")
    put("log.dir", embeddedKafkaPath.toString)
    put("num.partitions", numPartitions.toString)
    put("auto.create.topics.enable", "true")
    put("zookeeper.connect", zkConnect)
    put("offsets.topic.replication.factor", "1")
  })

  private val kafka = new KafkaServerStartable(kafkaConfig)
  kafka.startup()

  lazy val admin = AdminClient.create(Map[String, AnyRef]("bootstrap.servers" -> kafkaBootstrap).asJava)

  def createTopic(name: String): Unit = {
    admin.createTopics(List(new NewTopic(name, numPartitions, 1)) asJava).all().get(30, TimeUnit.SECONDS)
  }

  def listTopics: mutable.Set[String] = {
    admin.listTopics().names().get(1, TimeUnit.SECONDS).asScala
  }

  val tmpZkClient = new ZkClient(zkConnect, 5000, 6000, new ZkSerializer {
    def serialize(o: Object): Array[Byte] = o.toString.getBytes

    override def deserialize(bytes: Array[Byte]): Object = new String(bytes)
  })

  val broker = Broker.createBroker(1, tmpZkClient.readData[String]("/brokers/ids/1"))
  val kafkaBootstrap = broker.getBrokerEndPoint(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)).connectionString()
  tmpZkClient.close
  log.info(s"Embedded Kafka $kafkaBootstrap, data dir: $testDir")

  abstract override def afterAll(): Unit = try {
    kafka.shutdown()
  } finally {
    super.afterAll()
  }


}
