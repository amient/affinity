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

import java.util.Properties

import akka.actor.Actor
import akka.actor.Status.{Failure, Success}
import akka.event.Logging
import com.typesafe.config.Config
import io.amient.affinity.Conf
import io.amient.affinity.core.actor.{TransactionAbort, TransactionBegin, TransactionCommit, TransactionalRecord}
import io.amient.affinity.core.config.CfgStruct
import io.amient.affinity.core.storage.StorageConf
import io.amient.affinity.kafka.KafkaStorage.{KafkaConsumerConf, KafkaProducerConf}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.ByteArraySerializer

import scala.collection.JavaConverters._

object KafkaConf extends KafkaConf {
  override def apply(config: Config): KafkaConf = new KafkaConf().apply(config)
}

class KafkaConf extends CfgStruct[KafkaConf](classOf[StorageConf]) {
  val BootstrapServers = string("kafka.bootstrap.servers", true).doc("kafka connection string used for consumer and/or producer")
  val Producer = struct("kafka.producer", new KafkaProducerConf, false).doc("any settings that the underlying version of kafka producer client supports")
  val Consumer = struct("kafka.consumer", new KafkaConsumerConf, false).doc("any settings that the underlying version of kafka consumer client supports")
}

class TransactionalProducer extends Actor {

  val logger = Logging.getLogger(context.system, this)

  private[this] var producer: KafkaProducer[Array[Byte], Array[Byte]] = null

  val kafkaConf = KafkaConf(Conf(context.system.settings.config).Affi.Storage)
  val producerConfig = new Properties() {
    if (kafkaConf.Producer.isDefined) {
      val producerConfig = kafkaConf.Producer.toMap()
      if (producerConfig.containsKey("bootstrap.servers")) throw new IllegalArgumentException("bootstrap.servers cannot be overriden for KafkaStroage producer")
      if (producerConfig.containsKey("key.serializer")) throw new IllegalArgumentException("Binary kafka stream cannot use custom key.serializer")
      if (producerConfig.containsKey("value.serializer")) throw new IllegalArgumentException("Binary kafka stream cannot use custom value.serializer")
      producerConfig.entrySet.asScala.filter(_.getValue.isDefined).foreach { case (entry) =>
        put(entry.getKey, entry.getValue.apply.toString)
      }
    }
    put("bootstrap.servers", kafkaConf.BootstrapServers())
    put("value.serializer", classOf[ByteArraySerializer].getName)
    put("key.serializer", classOf[ByteArraySerializer].getName)
  }

  override def receive: Receive = {

    case req@TransactionBegin() => req(sender) ! {
      if (producer == null) {
        producerConfig.put("transactional.id", "my-unique-affinity-node-id") //TODO get affinity node id
        producer = new KafkaProducer[Array[Byte], Array[Byte]](producerConfig)
        logger.debug("Transactions.Init()")
        producer.initTransactions()
      }
      logger.debug("Transactions.Begin()")
      producer.beginTransaction()
    }

    case TransactionalRecord(topic, key, value, timestamp, partition) =>
      val replyto = sender
      val producerRecord = new ProducerRecord(
        topic,
        partition.map(new Integer(_)).getOrElse(null),
        timestamp.map(new java.lang.Long(_)).getOrElse(null),
        key,
        value)
      logger.debug(s"Transactions.Append($topic)")
      producer.send(producerRecord, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (exception != null) {
            replyto ! Failure(exception)
          } else {
            replyto ! Success(metadata.offset())
          }
        }
      })

    case req@TransactionCommit() => req(sender) ! {
      logger.debug("Transactions.Commit()")
      producer.commitTransaction()
    }

    case req@TransactionAbort() => req(sender) ! {
      logger.debug("Transactions.Abort()")
      producer.abortTransaction()
    }
  }
}