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
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{Future, TimeUnit}

import com.typesafe.config.Config
import io.amient.affinity.core.storage.{MemStore, Storage}
import io.amient.affinity.core.util.MappedJavaFuture
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.BrokerNotAvailableException
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.language.reflectiveCalls


object KafkaStorage {
  def CONFIG_KAFKA_BOOTSTRAP_SERVERS = "storage.kafka.bootstrap.servers"

  def CONFIG_KAFKA_TOPIC = s"storage.kafka.topic"

  def CONFIG_KAFKA_PRODUCER = s"storage.kafka.producer"

  def CONFIG_KAFKA_CONSUMER = s"storage.kafka.consumer"

  val log = LoggerFactory.getLogger(classOf[KafkaStorage])
}

class KafkaStorage(config: Config, partition: Int) extends Storage(config, partition) {

  import KafkaStorage._

  final val brokers: String = config.getString(CONFIG_KAFKA_BOOTSTRAP_SERVERS)
  final val topic: String = config.getString(CONFIG_KAFKA_TOPIC)
  final val ttlSec = if (config.hasPath(MemStore.CONFIG_TTL_SEC)) config.getInt(MemStore.CONFIG_TTL_SEC)  else -1
  private val producerProps = new Properties() {
    if (config.hasPath(CONFIG_KAFKA_PRODUCER)) {
      val producerConfig = config.getConfig(CONFIG_KAFKA_PRODUCER)
      if (producerConfig.hasPath("bootstrap.servers")) throw new IllegalArgumentException("bootstrap.servers cannot be overriden for KafkaStroage producer")
      if (producerConfig.hasPath("key.serializer")) throw new IllegalArgumentException("key.serializer cannot be overriden for KafkaStroage producer")
      if (producerConfig.hasPath("value.serializer")) throw new IllegalArgumentException("value.serializer cannot be overriden for KafkaStroage producer")
      producerConfig.entrySet().foreach { case (entry) =>
        put(entry.getKey, entry.getValue.unwrapped())
      }
    }
    put("bootstrap.servers", brokers)
    put("key.serializer", classOf[ByteArraySerializer].getName)
    put("value.serializer", classOf[ByteArraySerializer].getName)
  }

  require(producerProps.getProperty("acks", "1") != "0", "State store kafka producer acks cannot be configured to 0, at least 1 ack is required for consistency")

  val consumerProps = new Properties() {
    if (config.hasPath(CONFIG_KAFKA_CONSUMER)) {
      val consumerConfig = config.getConfig(CONFIG_KAFKA_CONSUMER)
      if (consumerConfig.hasPath("bootstrap.servers")) throw new IllegalArgumentException("bootstrap.servers cannot be overriden for KafkaStroage consumer")
      if (consumerConfig.hasPath("enable.auto.commit")) throw new IllegalArgumentException("enable.auto.commit cannot be overriden for KafkaStroage consumer")
      if (consumerConfig.hasPath("key.deserializer")) throw new IllegalArgumentException("key.deserializer cannot be overriden for KafkaStroage consumer")
      if (consumerConfig.hasPath("value.deserializer")) throw new IllegalArgumentException("value.deserializer cannot be overriden for KafkaStroage consumer")
      consumerConfig.entrySet().foreach { case (entry) =>
        put(entry.getKey, entry.getValue.unwrapped())
      }
    }
    put("bootstrap.servers", brokers)
    put("enable.auto.commit", "false")
    put("key.deserializer", classOf[ByteArrayDeserializer].getName)
    put("value.deserializer", classOf[ByteArrayDeserializer].getName)
  }

  ensureCorrectTopicConfiguiration()

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
              for (r <- records.iterator()) {
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

  private def ensureCorrectTopicConfiguiration() {
    val adminProps = new Properties() {
      put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, producerProps.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
    }
    val admin = AdminClient.create(adminProps)
    val adminTimeout = 5
    val topicConfigs = Map(
      //TODO #86 add all relevant configs described in the issue
      TopicConfig.CLEANUP_POLICY_CONFIG -> (if (ttlSec > 0) "compact,delete" else "compact"))

    //TODO #86 make sure that calling this from all partitions is synchronized on the broker
    if (!admin.listTopics().names().get(adminTimeout, TimeUnit.SECONDS).contains(topic)) {
      //TODO #86 number of partitions and repl. factor is defined for each state, not service!
      val schemaTopicRequest = new NewTopic(topic, numPartitions, replicationFactor)
      schemaTopicRequest.configs(topicConfigs)
      admin.createTopics(List(schemaTopicRequest)).all.get(adminTimeout, TimeUnit.MILLISECONDS)
    } else {
      //TODO #86 describe topic and alter configs if different from topicConfigs
    }
  }

}
