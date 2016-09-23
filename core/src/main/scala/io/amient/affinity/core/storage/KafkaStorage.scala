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
import java.util.concurrent.atomic.AtomicReference

import io.amient.affinity.core.serde.Serde
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

abstract class KafkaStorage[K, V](brokers: String,
                                  topic: String,
                                  partition: Int,
                                  keySerde: Class[_ <: Serde],
                                  valueSerde: Class[_ <: Serde]) extends Storage[K, V] {

  val producerProps = new Properties()
  producerProps.put("bootstrap.servers", brokers)
  producerProps.put("acks", "all")
  producerProps.put("retries", "0")
  producerProps.put("linger.ms", "0")
  producerProps.put("key.serializer", keySerde.getName)
  producerProps.put("value.serializer", valueSerde.getName)
  val kafkaProducer = new KafkaProducer[K, V](producerProps)

  private var tailing = true
  private val consumerError = new AtomicReference[Throwable](null)

  private val consumer = new Thread {
    val consumerProps = new Properties()
    consumerProps.put("bootstrap.servers", brokers)
    consumerProps.put("enable.auto.commit", "false")
    consumerProps.put("key.deserializer", keySerde.getName)
    consumerProps.put("value.deserializer", valueSerde.getName)
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
          val lastOffset = kafkaConsumer.position(tp)
          kafkaConsumer.seek(tp, bootOffset)

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
              //TODO #6 the fact that fetchedNumRecords is 0 doesn't still guarantee that there were no other records
              //produced by another instance - to reliably know that the consumer is fully caught up,
              // there should be a watermark maintained by the master which is updated similarly to standard
              // kafka consumer offset
              if (!tailing && fetchedNumRecrods == 0) { //&& kafkaConsumer.position(tp) >= lastOffset) {
                keepConsuming = false
              }
            } catch {
              case e: Throwable =>
                e.printStackTrace()
                synchronized {
                  consumerError.set(e)
                  notify
                }
                this.interrupt()
            }
          }

          synchronized {
            notify()
//            println(s"`$topic`, partition $partition suspending tail consumption @ offest ${kafkaConsumer.position(tp)}")
            wait()
//            println(s"`$topic`, partition $partition resuming tail consumption from offset ${kafkaConsumer.position(tp)}")
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

  def write(key: K, value: V): java.util.concurrent.Future[RecordMetadata] = {
    kafkaProducer.send(new ProducerRecord(topic, partition, key, value))
  }

}
