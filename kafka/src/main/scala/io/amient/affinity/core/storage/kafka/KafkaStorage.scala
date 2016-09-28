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

import java.nio.ByteBuffer
import java.util
import java.util.Properties
import java.util.concurrent.atomic.AtomicReference

import com.typesafe.config.Config
import io.amient.affinity.core.storage.Storage
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteBufferDeserializer, ByteBufferSerializer}

import scala.collection.JavaConverters._

object KafkaStorage {
  def CONFIG_KAFKA_BOOTSTRAP_SERVERS(name: String) = s"affinity.state.$name.kafka.bootstrap.servers"
  def CONFIG_KAFKA_TOPIC(name: String) = s"affinity.state.$name.kafka.topic"
  //TODO this should be part of partition assignment process not config
  def CONFIG_KAFKA_PARTITION(name: String) = s"affinity.state.$name.kafka.partition"
}

abstract class KafkaStorage(name: String, config: Config) extends Storage(name, config) {

  import KafkaStorage._

  final val brokers: String = config.getString(CONFIG_KAFKA_BOOTSTRAP_SERVERS(name))
  final val topic: String = config.getString(CONFIG_KAFKA_TOPIC(name))
  final val partition: Int = config.getInt(CONFIG_KAFKA_PARTITION(name))

  val producerProps = new Properties()
  producerProps.put("bootstrap.servers", brokers)
  //TODO anything under affinity.state.$name.kafka.producer._ should go here
  producerProps.put("acks", "all")
  producerProps.put("retries", "0")
  producerProps.put("linger.ms", "0")
  producerProps.put("key.serializer", classOf[ByteBufferSerializer].getName)
  producerProps.put("value.serializer", classOf[ByteBufferSerializer].getName)
  val kafkaProducer = new KafkaProducer[ByteBuffer, ByteBuffer](producerProps)

  private var tailing = true
  private val consumerError = new AtomicReference[Throwable](null)

  private val consumer = new Thread {
    val consumerProps = new Properties()
    consumerProps.put("bootstrap.servers", brokers)
    consumerProps.put("enable.auto.commit", "false")
    consumerProps.put("key.deserializer", classOf[ByteBufferDeserializer].getName)
    consumerProps.put("value.deserializer", classOf[ByteBufferDeserializer].getName)
    val kafkaConsumer = new KafkaConsumer[ByteBuffer, ByteBuffer](consumerProps)

    val tp = new TopicPartition(topic, partition)
    val consumerPartitions = util.Arrays.asList(tp)
    kafkaConsumer.assign(consumerPartitions)
    kafkaConsumer.seekToBeginning(consumerPartitions)


    override def run(): Unit = {

      try {
        while (true) {

          if (isInterrupted) throw new InterruptedException

          var keepConsuming = true

          while (keepConsuming) {

            if (isInterrupted) throw new InterruptedException

            try {
              val records = kafkaConsumer.poll(500)
              var fetchedNumRecrods = 0
              for (r <- records.iterator().asScala) {
                fetchedNumRecrods += 1
                if (r.value == null) {
                  remove(r.key)
                } else {
                  update(r.key, r.value)
                }
              }
              if (!tailing && fetchedNumRecrods == 0) {
                keepConsuming = false
              }
            } catch {
              case e: Throwable =>
                e.printStackTrace()
                synchronized {
                  consumerError.set(e)
                  notify
                }
            }
          }

          synchronized {
            notify()
            wait()
          }

        }
      } catch {
        case e: InterruptedException => kafkaConsumer.close()
      }
    }
  }

  private[core] def init(): Unit = consumer.start()

  private[core] def boot(): Unit = {
    consumer.synchronized {
      if (tailing) {
        tailing = false
        //TODO #12 instead of infinite wait do interval wait with health-check
        // the health check cannot be trivial because kafka may block infinitely when corruption occurs in the broker
        consumer.synchronized(consumer.wait)
        if (consumerError.get != null) {
          throw consumerError.get
        }
      }
    }
  }

  private[core] def tail(): Unit = {
    consumer.synchronized {
      if (!tailing) {
        tailing = true
        consumer.notify
      }
    }
  }

  private[core] def close(): Unit = {
    //stop tailing and shutdown
    try {
      consumer.interrupt()
    } finally {
      kafkaProducer.close()
    }
  }

  def write(key: MK, value: MV): java.util.concurrent.Future[RecordMetadata] = {
    kafkaProducer.send(new ProducerRecord(topic, partition, key, value))
  }

}
