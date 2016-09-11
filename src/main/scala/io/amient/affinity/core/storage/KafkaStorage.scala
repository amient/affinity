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

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}

import scala.collection.JavaConverters._

abstract class KafkaStorage[K, V](topic: String, partition: Int) extends Storage[K, V] {

  val producerProps = new Properties()
  producerProps.put("bootstrap.servers", "localhost:9092")
  producerProps.put("acks", "all")
  producerProps.put("retries", "0")
  producerProps.put("linger.ms", "0")
  producerProps.put("key.serializer", classOf[ByteArraySerializer].getName)
  producerProps.put("value.serializer", classOf[ByteArraySerializer].getName)
  val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps)

  def boot(isLeader: () => Boolean): Unit = {

    //TODO configure KafkaStorage via appConfig and replace prinln(s) with log.info
    println(s"`$topic` topic bootstrapping memstore partition $partition")
    val consumerProps = new Properties()
    consumerProps.put("bootstrap.servers", "localhost:9092")
    consumerProps.put("enable.auto.commit", "false")
    consumerProps.put("key.deserializer", classOf[ByteArrayDeserializer].getName)
    consumerProps.put("value.deserializer", classOf[ByteArrayDeserializer].getName)

    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerProps)
    try {
      val tp = new TopicPartition(topic, partition)
      val consumerPartitions = util.Arrays.asList(tp)
      consumer.assign(consumerPartitions)
      consumer.seekToEnd(consumerPartitions)
      consumer.poll(50L)
      val lastOffset = consumer.position(tp)
      println(s"`$topic` kafka topic, partition: $partition, latest offset: $lastOffset")

      consumer.seekToBeginning(consumerPartitions)
      val continue = new AtomicBoolean(true)
      while (continue.get) {
        val records = consumer.poll(1000)
        for (r <- records.iterator().asScala) {
          val (key, value) = deserialize(r.key(), r.value())
          r.value() match {
            case null => remove(key)
            case any => update(key, value)
          }
        }
        if (consumer.position(tp) >= lastOffset) {
          if (isLeader()) continue.set(false)
        }
      }
    } finally {
      consumer.close
      println(s"`$topic`, partition $partition bootstrap completed")
      //TODO after becoming a master there can only be termination because we're closing the consumer
    }
  }

  def write(kv: (Array[Byte], Array[Byte])): java.util.concurrent.Future[RecordMetadata] = {
    producer.send(new ProducerRecord(topic, partition, kv._1, kv._2))
  }


}

