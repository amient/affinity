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

import com.typesafe.config.{Config, ConfigValueFactory}
import io.amient.affinity.core.ack
import io.amient.affinity.core.actor.{Partition, Service}
import io.amient.affinity.core.storage.State
import io.amient.affinity.core.storage.kafka.KafkaStorage
import io.amient.affinity.core.util.Reply
import io.amient.affinity.kafka.EmbeddedKafka

import scala.collection.JavaConverters._

trait SystemTestBaseWithKafka extends SystemTestBaseWithZk with EmbeddedKafka {

  override def configure(config: Config): Config = super.configure(config)
    .withValue(Service.CONFIG_NUM_PARTITIONS, ConfigValueFactory.fromAnyRef(2)) match {
    case cfg if (!cfg.hasPath(State.CONFIG_STATE)) => cfg
    case cfg =>
      cfg.getConfig(State.CONFIG_STATE).entrySet().asScala
        .map(entry => (entry.getKey, entry.getValue.unwrapped().toString))
        .filter { case (p, c) => p.endsWith("storage.class") && classOf[KafkaStorage].isAssignableFrom(Class.forName(c)) }
        .map { case (p, c) => (State.CONFIG_STATE + "." + p.split("\\.")(0), c) }
        .foldLeft(cfg) { case (cfg, (p, c)) =>
          cfg.withValue(p + "." + KafkaStorage.CONFIG_KAFKA_BOOTSTRAP_SERVERS, ConfigValueFactory.fromAnyRef(kafkaBootstrap))
        }
  }
  class MyTestPartition(topic: String) extends Partition {

    import MyTestPartition._
    import context.dispatcher

    private val stateConfig = context.system.settings.config.getConfig(State.CONFIG_STATE_STORE(topic))
      .withValue(KafkaStorage.CONFIG_KAFKA_TOPIC, ConfigValueFactory.fromAnyRef(topic))

    val data = state {
      new State[String, String](topic, context.system, stateConfig)
    }

    override def handle: Receive = {
      case request@GetValue(key) => sender.reply(request) {
        data(key)
      }

      case request@PutValue(key, value) => sender.replyWith(request) {
        data.update(key, value)
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

