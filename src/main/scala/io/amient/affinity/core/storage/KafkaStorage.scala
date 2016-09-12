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

package io.amient.affinity.core.storage

import java.util
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import io.amient.affinity.example.data.AvroSerde
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

abstract class KafkaStorage[K, V](topic: String, partition: Int) extends Storage[K, V] {

  //TODO configure KafkaStorage via appConfig and replace prinln(s) with log.info
  val keySerde = classOf[AvroSerde].getName
  val valueSerde = classOf[AvroSerde].getName
  //TODO provide serde via appConfig

  val producerProps = new Properties()
  producerProps.put("bootstrap.servers", "localhost:9092")
  producerProps.put("acks", "all")
  producerProps.put("retries", "0")
  producerProps.put("linger.ms", "0")
  producerProps.put("key.serializer", keySerde)
  producerProps.put("value.serializer", valueSerde)
  val kafkaProducer = new KafkaProducer[K, V](producerProps)

  val doTail = new AtomicBoolean(true)

  private val consumer = new Thread {

    val consumerProps = new Properties()
    consumerProps.put("bootstrap.servers", "localhost:9092")
    consumerProps.put("enable.auto.commit", "false")
    consumerProps.put("key.deserializer", keySerde)
    consumerProps.put("value.deserializer", valueSerde)
    val kafkaConsumer = new KafkaConsumer[K, V](consumerProps)
    val tp = new TopicPartition(topic, partition)
    val consumerPartitions = util.Arrays.asList(tp)
    kafkaConsumer.assign(consumerPartitions)
    kafkaConsumer.seekToBeginning(consumerPartitions)

    override def run(): Unit = {

      try {
        while (true) {

          if (isInterrupted) throw new InterruptedException

          val bootOffset = kafkaConsumer.position(tp)
          kafkaConsumer.seekToEnd(consumerPartitions)
          kafkaConsumer.poll(50L)
          val lastOffset = kafkaConsumer.position(tp)
          kafkaConsumer.seek(tp, bootOffset)
          if (doTail.get) {
            println(s"tailing memstore topic `$topic` partition $partition from offset [$bootOffset through $lastOffset to ...]")
          } else {
            println(s"booting memstore topic `$topic` partition $partition from offset [$bootOffset to $lastOffset]")
          }

          val isConsuming = new AtomicBoolean(true)

          while (isConsuming.get) {

            if (isInterrupted) throw new InterruptedException

            val records = kafkaConsumer.poll(1000)
            for (r <- records.iterator().asScala) {
              if (r.value == null) {
                remove(r.key)
              } else {
                update(r.key, r.value)
              }
            }
            if (!doTail.get) {
              if (kafkaConsumer.position(tp) >= lastOffset) {
                isConsuming.set(false)
              }
            }
          }

          synchronized {
            if (!doTail.get) {
              println(s"`$topic`, partition $partition bootstrap completed")
              notify()
            }
            notify
            wait
          }

        }
      } catch {
        case e: InterruptedException => kafkaConsumer.close()
      }
    }
  }

  def boot(): Unit = {
    consumer.start()
    consumer.synchronized {
      doTail.set(false)
      consumer.wait()
    }
  }

  def tail(): Unit = {
//    consumer.synchronized {
//      doTail.set(true)
//      consumer.notify
//    }
  }

  def close(): Unit = {
    try {
      kafkaProducer.close()
    } finally {
      consumer.interrupt()
    }
  }

  def write(key: K, value: V): java.util.concurrent.Future[RecordMetadata] = {
    kafkaProducer.send(new ProducerRecord(topic, partition, key, value))
  }

}
