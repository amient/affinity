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

import com.typesafe.config.{Config, ConfigValueFactory}
import io.amient.affinity.core.actor.Partition
import io.amient.affinity.core.storage.State
import io.amient.affinity.core.ack._
import io.amient.affinity.core.storage.kafka.KafkaStorage
import io.amient.affinity.core.util.ZooKeeperClient
import kafka.cluster.Broker
import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.apache.kafka.common.protocol.SecurityProtocol

import scala.collection.JavaConverters._

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

  override def configure(config: Config): Config = super.configure(config) match {
    case cfg if (!cfg.hasPath(State.CONFIG_STATE)) => cfg
    case cfg =>
      cfg.getConfig(State.CONFIG_STATE).entrySet().asScala
        .map(entry => (entry.getKey, entry.getValue.unwrapped().toString))
        .filter { case (p, c) => p.endsWith("storage.class")  && classOf[KafkaStorage].isAssignableFrom(Class.forName(c)) }
        .map { case (p, c) => (State.CONFIG_STATE + "." + p.split("\\.")(0) , c) }
        .foldLeft(cfg) { case (cfg, (p, c)) =>
          cfg.withValue(p + "." + KafkaStorage.CONFIG_KAFKA_BOOTSTRAP_SERVERS, ConfigValueFactory.fromAnyRef(kafkaBootstrap))
        }
  }

  override def afterAll(): Unit = {
    try {
      kafka.shutdown()
    } catch {
      case e: IllegalStateException => //
    }
    super.afterAll()
  }

  class MyTestPartition(topic: String) extends Partition {

    import MyTestPartition._

    import context.dispatcher

    private val stateConfig = context.system.settings.config.getConfig(State.CONFIG_STATE_STORE(topic))
      .withValue(KafkaStorage.CONFIG_KAFKA_TOPIC, ConfigValueFactory.fromAnyRef(topic))

    val data = state {
      new State[String, String](context.system, stateConfig)
    }

    override def handle: Receive = {
      case request @ GetValue(key) => reply(request, sender) {
        data(key)
      }

      case request @ PutValue(key, value) => replyWith(request, sender) {
        data.put(key, value)
      }
    }
  }

}

object MyTestPartition {

  case class GetValue(key: String) extends Reply[Option[String]] {
    override def hashCode(): Int = key.hashCode
  }

  case class PutValue(key: String, value: String) extends Reply[Option[String]] {
    override def hashCode(): Int = key.hashCode
  }

}

