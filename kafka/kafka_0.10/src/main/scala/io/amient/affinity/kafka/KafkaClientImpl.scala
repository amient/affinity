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
import java.util.Properties

import io.amient.affinity.core.util.ByteUtils
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.BrokerNotAvailableException
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.collection.JavaConverters._
import scala.util.Random

class KafkaClientImpl(val topic: String, @transient props: Properties) extends KafkaClient {

  val config = new Properties() with Serializable {
    put("value.deserializer", classOf[ByteArrayDeserializer].getName)
    put("key.deserializer", classOf[ByteArrayDeserializer].getName)
    put("enable.auto.commit", "false")
    putAll(props)
  }

  //  val clientId = config.getProperty("client.id", "")
  val brokers = config.getProperty("bootstrap.servers", "localhost:9092")
  val brokerList = brokers.split(",").toList
  //  val socketTimeoutMs = config.getProperty("socket.timeout.ms", "30000").toInt
  //  val socketReceiveBufferBytes = config.getProperty("socket.receive.buffer.bytes", "65536")
  //  val fetchMessageMaxBytes = config.getProperty("fetch.message.max.bytes", "1048576")


  override def topicOffsets(time: Long): util.Map[Integer, Long] = {
    metadata((consumer) => {
      val tp = consumer.partitionsFor(topic).asScala.map(info => new TopicPartition(topic, info.partition())).asJava
      consumer.assign(tp)
      if (time == KafkaClient.EARLIEST_TIME) {
        consumer.seekToBeginning(tp)
      } else if (time == KafkaClient.LATEST_TIME) {
        consumer.seekToEnd(tp)
      } else {
        throw new IllegalArgumentException(s"Invalid offset limit, expecting either ${KafkaClient.EARLIEST_TIME} or ${KafkaClient.LATEST_TIME}")
      }

      tp.asScala.map {
        t => new Integer(t.partition()) -> new Long(consumer.position(t))
      }.toMap.asJava
    })
  }

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

  override def close(): Unit = ??? //TODO close all iterators that are still open

  override def iterator(partition: Int, startOffset: Long, stopOffset: Long): util.Iterator[KeyPayloadAndOffset] = {

    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](config)
    val tp = new TopicPartition(topic, partition)
    consumer.assign(List(tp).asJava)

    new util.Iterator[KeyPayloadAndOffset] {
      private var record: ConsumerRecord[Array[Byte], Array[Byte]] = null
      private var offset = startOffset
      private var setIter: util.Iterator[ConsumerRecord[Array[Byte], Array[Byte]]] = null

      seek

      override def hasNext: Boolean = record != null

      override def next(): KeyPayloadAndOffset = {
        if (record == null) {
          throw new NoSuchElementException
        } else {
          val k = new ByteKey(record.key)
          val v = new PayloadAndOffset(record.offset, record.value)
          seek
          new KeyPayloadAndOffset(k, v)
        }
      }

      private def seek {
        while (offset < stopOffset) {
          consumer.seek(tp, offset)
          if (setIter == null || !setIter.hasNext) setIter = consumer.poll(1000).iterator() //TODO configurable timeout
          val u: ConsumerRecord[Array[Byte], Array[Byte]] = setIter.next()
          record = setIter.next()
          offset = record.offset + 1L
          if (record.value != null && record.offset >= startOffset) {
            return
          }
        }
        record = null
      }

    }
  }

}
