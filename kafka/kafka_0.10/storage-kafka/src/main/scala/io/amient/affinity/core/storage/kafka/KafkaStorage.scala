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

package io.amient.affinity.core.storage.kafka

import java.util
import java.util.Properties
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicReference

import com.typesafe.config.Config
import io.amient.affinity.core.config.{Cfg, CfgStruct}
import io.amient.affinity.core.storage.Storage.StorageConf
import io.amient.affinity.core.storage.{StateConf, Storage}
import io.amient.affinity.core.util.MappedJavaFuture
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.BrokerNotAvailableException
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.language.reflectiveCalls


object KafkaStorage {

  object KafkaStorageConf extends KafkaStorageConf {
    override def apply(config: Config): KafkaStorageConf = new KafkaStorageConf().apply(config)
  }

  class KafkaStorageConf extends CfgStruct[KafkaStorageConf](classOf[StorageConf]) {
    val Topic = string("kafka.topic", true)
    val BootstrapServers = string("kafka.bootstrap.servers", true)
    val Producer = struct("kafka.producer", new KafkaProducerConf, false)
    val Consumer = struct("kafka.consumer", new KafkaConsumerConf, false)

  }

  class KafkaProducerConf extends CfgStruct[KafkaProducerConf](Cfg.Options.IGNORE_UNKNOWN)

  class KafkaConsumerConf extends CfgStruct[KafkaConsumerConf](Cfg.Options.IGNORE_UNKNOWN)

  val log = LoggerFactory.getLogger(classOf[KafkaStorage])
}

class KafkaStorage(stateConf: StateConf, partition: Int) extends Storage(stateConf, partition) {

  import KafkaStorage._

  private val conf = KafkaStorageConf(stateConf.Storage)

  val topic = conf.Topic()

  private val producerProps = new Properties() {
    if (conf.Producer.isDefined) {
      val producerConfig = conf.Producer.config()
      if (producerConfig.hasPath("bootstrap.servers")) throw new IllegalArgumentException("bootstrap.servers cannot be overriden for KafkaStroage producer")
      if (producerConfig.hasPath("key.serializer")) throw new IllegalArgumentException("key.serializer cannot be overriden for KafkaStroage producer")
      if (producerConfig.hasPath("value.serializer")) throw new IllegalArgumentException("value.serializer cannot be overriden for KafkaStroage producer")
      producerConfig.entrySet().asScala.foreach { case (entry) =>
        put(entry.getKey, entry.getValue.unwrapped())
      }
    }
    put("bootstrap.servers", conf.BootstrapServers())
    put("key.serializer", classOf[ByteArraySerializer].getName)
    put("value.serializer", classOf[ByteArraySerializer].getName)
  }

  require(producerProps.getProperty("acks", "1") != "0", "State store kafka producer acks cannot be configured to 0, at least 1 ack is required for consistency")

  val consumerProps = new Properties() {
    if (conf.Consumer.isDefined) {
      val consumerConfig = conf.Consumer.config()
      if (consumerConfig.hasPath("bootstrap.servers")) throw new IllegalArgumentException("bootstrap.servers cannot be overriden for KafkaStroage consumer")
      if (consumerConfig.hasPath("enable.auto.commit")) throw new IllegalArgumentException("enable.auto.commit cannot be overriden for KafkaStroage consumer")
      if (consumerConfig.hasPath("key.deserializer")) throw new IllegalArgumentException("key.deserializer cannot be overriden for KafkaStroage consumer")
      if (consumerConfig.hasPath("value.deserializer")) throw new IllegalArgumentException("value.deserializer cannot be overriden for KafkaStroage consumer")
      consumerConfig.entrySet().asScala.foreach { case (entry) =>
        put(entry.getKey, entry.getValue.unwrapped())
      }
    }
    put("bootstrap.servers", conf.BootstrapServers())
    put("enable.auto.commit", "false")
    put("key.deserializer", classOf[ByteArrayDeserializer].getName)
    put("value.deserializer", classOf[ByteArrayDeserializer].getName)
  }

  protected val kafkaProducer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps)

  @volatile private var tailing = true

  @volatile private var consuming = false

  private val consumerError = new AtomicReference[Throwable](null)

  private val consumer = new Thread {

    val kafkaConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerProps)

    val tp = new TopicPartition(topic, partition)
    val consumerPartitions = util.Arrays.asList(tp)
    kafkaConsumer.assign(consumerPartitions)
    memstore.getCheckpoint match {
      case checkpoint if checkpoint.offset <= 0 =>
        log.info(s"Rewinding into $tp")
        kafkaConsumer.seekToBeginning(consumerPartitions)
      case checkpoint =>
        log.info(s"Seeking ${checkpoint.offset} into $tp")
        kafkaConsumer.seek(tp, checkpoint.offset)
    }

    override def run(): Unit = {

      try {
        while (true) {

          if (isInterrupted) throw new InterruptedException

          consuming = true
          while (consuming) {

            if (isInterrupted) throw new InterruptedException

            consumerError.set(new BrokerNotAvailableException("Could not connect to Kafka"))
            try {
              val records = kafkaConsumer.poll(500)
              consumerError.set(null)
              var fetchedNumRecrods = 0
              for (r <- records.iterator().asScala) {
                fetchedNumRecrods += 1
                if (r.value == null) {
                  memstore.unload(r.key, r.offset())
                } else {
                  memstore.load(r.key, r.value, r.offset(), r.timestamp())
                }
              }
              if (!tailing && fetchedNumRecrods == 0) {
                consuming = false
              }
            } catch {
              case e: Throwable =>
                synchronized {
                  consumerError.set(e)
                  notify //boot failure
                }
            }
          }

          synchronized {
            notify() //boot complete
            wait() //wait for tail instruction
          }
        }
      } catch {
        case e: InterruptedException => return
      } finally {
        kafkaConsumer.close()
      }
    }
  }

  private[affinity] def init(): Unit = consumer.start()

  private[affinity] def boot(): Unit = {
    consumer.synchronized {
      if (tailing) {
        tailing = false
        while (true) {
          consumer.wait(6000)
          if (consumerError.get != null) {
            consumer.kafkaConsumer.wakeup()
            throw consumerError.get
          } else if (!consuming) {
            return
          }
        }
      }
    }
  }

  private[affinity] def tail(): Unit = {
    consumer.synchronized {
      if (!tailing) {
        tailing = true
        consumer.notify
      }
    }
  }

  override protected def stop(): Unit = {
    try {
      consumer.interrupt()
    } finally {
      kafkaProducer.close()
    }
  }

  def write(key: Array[Byte], value: Array[Byte], timestamp: Long): Future[java.lang.Long] = {
    new MappedJavaFuture[RecordMetadata, java.lang.Long](kafkaProducer.send(new ProducerRecord(topic, partition, timestamp, key, value))) {
      override def map(result: RecordMetadata): java.lang.Long = result.offset()
    }
  }

  def delete(key: Array[Byte]): Future[java.lang.Long] = {
    new MappedJavaFuture[RecordMetadata, java.lang.Long](kafkaProducer.send(new ProducerRecord(topic, partition, key, null))) {
      override def map(result: RecordMetadata): java.lang.Long = result.offset()
    }
  }

}
