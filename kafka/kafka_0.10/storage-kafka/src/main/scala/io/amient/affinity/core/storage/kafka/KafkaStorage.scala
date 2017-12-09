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
import java.util.concurrent.{CompletableFuture, Future}
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Supplier

import com.typesafe.config.Config
import io.amient.affinity.core.storage.Storage
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.BrokerNotAvailableException
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}

import scala.collection.JavaConverters._
import scala.concurrent.java8.FuturesConvertersImpl
import scala.language.reflectiveCalls


object KafkaStorage {
  def CONFIG_KAFKA_BOOTSTRAP_SERVERS = "storage.kafka.bootstrap.servers"

  def CONFIG_KAFKA_TOPIC = s"storage.kafka.topic"

  def CONFIG_KAFKA_PRODUCER = s"storage.kafka.producer"

  def CONFIG_KAFKA_CONSUMER = s"storage.kafka.consumer"
}

class KafkaStorage(config: Config, partition: Int) extends Storage(config, partition) {

  import KafkaStorage._

  final val brokers: String = config.getString(CONFIG_KAFKA_BOOTSTRAP_SERVERS)
  final val topic: String = config.getString(CONFIG_KAFKA_TOPIC)

  private val producerProps = new Properties() {
    if (config.hasPath(CONFIG_KAFKA_PRODUCER)) {
      val producerConfig = config.getConfig(CONFIG_KAFKA_PRODUCER)
      if (producerConfig.hasPath("bootstrap.servers")) throw new IllegalArgumentException("bootstrap.servers cannot be overriden for KafkaStroage producer")
      if (producerConfig.hasPath("key.serializer")) throw new IllegalArgumentException("key.serializer cannot be overriden for KafkaStroage producer")
      if (producerConfig.hasPath("value.serializer")) throw new IllegalArgumentException("value.serializer cannot be overriden for KafkaStroage producer")
      producerConfig.entrySet().asScala.foreach { case (entry) =>
        put(entry.getKey, entry.getValue.unwrapped())
      }
    }
    put("bootstrap.servers", brokers)
    put("key.serializer", classOf[ByteArraySerializer].getName)
    put("value.serializer", classOf[ByteArraySerializer].getName)
  }

  val consumerProps = new Properties() {
    if (config.hasPath(CONFIG_KAFKA_CONSUMER)) {
      val consumerConfig = config.getConfig(CONFIG_KAFKA_CONSUMER)
      if (consumerConfig.hasPath("bootstrap.servers")) throw new IllegalArgumentException("bootstrap.servers cannot be overriden for KafkaStroage consumer")
      if (consumerConfig.hasPath("enable.auto.commit")) throw new IllegalArgumentException("enable.auto.commit cannot be overriden for KafkaStroage consumer")
      if (consumerConfig.hasPath("key.deserializer")) throw new IllegalArgumentException("key.deserializer cannot be overriden for KafkaStroage consumer")
      if (consumerConfig.hasPath("value.deserializer")) throw new IllegalArgumentException("value.deserializer cannot be overriden for KafkaStroage consumer")
      consumerConfig.entrySet().asScala.foreach { case (entry) =>
        put(entry.getKey, entry.getValue.unwrapped())
      }
    }
    put("bootstrap.servers", brokers)
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
    kafkaConsumer.seekToBeginning(consumerPartitions)

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
                  memstore.unload(r.key)
                } else {
                  memstore.load(r.key, r.value, r.timestamp())
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
    val jf = kafkaProducer.send(new ProducerRecord(topic, partition, timestamp, key, value))
    CompletableFuture.supplyAsync(new Supplier[java.lang.Long] {
      override def get() = jf.get.offset()
    })
  }

  def delete(key: Array[Byte]): Future[java.lang.Long] = {
    val jf = kafkaProducer.send(new ProducerRecord(topic, partition, key, null))
    CompletableFuture.supplyAsync(new Supplier[java.lang.Long] {
      override def get() = jf.get.offset()
    })
  }

}
