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

import java.lang.Long
import java.util
import java.util.{Optional, Properties}

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.BrokerNotAvailableException

import scala.collection.JavaConverters._
import scala.util.Random

class KafkaClientImpl(@transient props: Properties, val topic: String) extends KafkaClient {

  val config = new Properties() with Serializable {
    putAll(props)
  }

  //  val clientId = config.getProperty("client.id", "")
  val brokers = config.getProperty("metadata.broker.list", "localhost:9092")
  val brokerList = brokers.split(",").toList
  //  val socketTimeoutMs = config.getProperty("socket.timeout.ms", "30000").toInt
  //  val socketReceiveBufferBytes = config.getProperty("socket.receive.buffer.bytes", "65536")
  //  val fetchMessageMaxBytes = config.getProperty("fetch.message.max.bytes", "1048576")

  implicit def OptionalKafkaBroker(opt: Option[KafkaBroker]): Optional[KafkaBroker] = {
    if (opt.isDefined) Optional.of(opt.get) else Optional.empty()
  }

  override def refreshLeaderBackoffMs(): Int = config.getProperty("refresh.leader.backoff.ms", "1000").toInt

  override def refreshLeaderMaxRetries(): Int = config.getProperty("refresh.leader.max.retries", "10").toInt

  override def getLeaders(): util.Map[Integer, Optional[KafkaBroker]] = metadata((consumer) => {
    consumer.partitionsFor(topic).asScala.map { partitionInfo =>
      val leader = partitionInfo.leader()
      val broker: Optional[KafkaBroker] = if (leader == null) None else Some(new KafkaBroker(leader.id()))
      (new Integer(partitionInfo.partition()), broker)
    }.toMap.asJava
  })

  override def topicOffsets(time: Long, leaders: util.Map[Integer, Optional[KafkaBroker]]): util.Map[Integer, Long] = {
    ???
  }

  override def connect(broker: KafkaBroker, tap: KafkaTopicAndPartition): KafkaFetcher = ???

  private def metadata[E](e: (KafkaConsumer[Array[Byte], Array[Byte]]) => E): E = {
    val it = Random.shuffle(brokerList).iterator.flatMap { broker =>
      try {
        val (host: String, port: Int) = broker.split(":") match {
          case Array(host) => (host, 9092)
          case Array(host, port) => (host, port.toInt)
        }
        val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](config)
        try {
          Some(e(consumer))
        } finally {
          consumer.close()
        }
      } catch {
        case e: Throwable => {
          e.printStackTrace()
          None
        }
      }
    }
    if (it.hasNext) it.next else throw new BrokerNotAvailableException("operation failed for all brokers")
  }

}
