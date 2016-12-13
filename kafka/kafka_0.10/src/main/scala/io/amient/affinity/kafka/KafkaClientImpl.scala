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

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.BrokerNotAvailableException
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

class KafkaClientImpl(val topic: String, props: Properties) extends KafkaClient {


  val consumerConfig = new Properties() with Serializable {
    putAll(props)
    put("value.deserializer", classOf[ByteArrayDeserializer].getName)
    put("key.deserializer", classOf[ByteArrayDeserializer].getName)
    put("enable.auto.commit", "false")
    //  val clientId = config.getProperty("client.id", "")
    //  val socketTimeoutMs = config.getProperty("socket.timeout.ms", "30000").toInt
    //  val socketReceiveBufferBytes = config.getProperty("socket.receive.buffer.bytes", "65536")
    //  val fetchMessageMaxBytes = config.getProperty("fetch.message.max.bytes", "1048576")
  }

  val brokers = consumerConfig.getProperty("bootstrap.servers", "localhost:9092")
  val brokerList = brokers.split(",").toList

  val producerConfig = new Properties() {
    put("bootstrap.servers", brokers)
    put("value.serializer", classOf[ByteArraySerializer].getName)
    put("key.serializer", classOf[ByteArraySerializer].getName)
    //    put("compression.codec", "snappy")
    //    put("linger.ms", "200")
    //    put("batch.size", "1000")
    //    put("acks", "0")
  }

  override def toString: String = s"Kafka[topic: $topic, brokers: $brokers, version: 0.10]"

  override def getPartitions = metadata(consumer => {
    consumer.partitionsFor(topic).asScala.map(info => new Integer(info.partition)).asJava
  })

  override def getOffsets(partition: Int): util.Map[Long, Long] = metadata((consumer) => {
    val tp = List(new TopicPartition(topic, partition)).asJava
    consumer.assign(tp)
    consumer.seekToBeginning(tp)
    val start = new Long(consumer.position(tp.get(0)))
    consumer.seekToEnd(tp)
    val end = new Long(consumer.position(tp.get(0)))
    Map(start -> end).asJava
  })

  private def metadata[E](e: (KafkaConsumer[Array[Byte], Array[Byte]]) => E): E = {
    val it = Random.shuffle(brokerList).iterator.flatMap { broker =>
      try {
        val (host: String, port: Int) = broker.split(":") match {
          case Array(host) => (host, 9092)
          case Array(host, port) => (host, port.toInt)
        }
        val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerConfig)
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

  private val consumers = scala.collection.mutable.Map[util.Iterator[KeyPayloadAndOffset], KafkaConsumer[_, _]]()

  override def release(iter: util.Iterator[KeyPayloadAndOffset]): Unit = {
    consumers.get(iter).foreach(_.close())
  }

  override def iterator(partition: Int, startOffset: Long, stopOffset: Long): util.Iterator[KeyPayloadAndOffset] = {

    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerConfig)

    val tp = new TopicPartition(topic, partition)
    consumer.assign(List(tp).asJava)

    val result = new util.Iterator[KeyPayloadAndOffset] {
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
          if (setIter.hasNext) {
            record = setIter.next()
            offset = record.offset + 1L
            if (record.offset >= startOffset) {
              return
            }
          }
        }
        record = null
      }

    }

    consumers += result -> consumer
    result
  }

  override def publish(iter: util.Iterator[KeyPayloadAndOffset],
                       checker: java.util.function.Function[java.lang.Long, java.lang.Boolean]): Unit = {
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerConfig)
    try {
      var messages = 0L
      while (iter.hasNext) {
        val kpo = iter.next()
        producer.send(new ProducerRecord(topic, kpo.payloadAndOffset.offset.toInt, kpo.key.bytes, kpo.payloadAndOffset.bytes))
        messages += 1
      }
      if (!checker(messages)) sys.error("Kafka iterator producer interrupted")
    } finally {
      producer.close()
    }
  }
}
