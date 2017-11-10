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

package io.amient.util.spark

import io.amient.affinity.core.serde.AbstractSerde
import io.amient.affinity.core.util.ObjectHashPartitioner
import io.amient.affinity.kafka._
import io.amient.affinity.spark.KafkaSplit
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.util.collection.ExternalAppendOnlyMap
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class KafkaRDD[K: ClassTag, V: ClassTag](sc: SparkContext,
                                         client: KafkaClient,
                                         keySerde: => AbstractSerde[_ >: K],
                                         valueSerde: => AbstractSerde[_ >: V],
                                         compacted: Boolean = false) extends RDD[(K, V)](sc, Nil) {

  def this(sc: SparkContext, client: KafkaClient, serde: => AbstractSerde[Any], compacted: Boolean) =
    this(sc, client, serde, serde, compacted)


  val compactor = (m1: PayloadAndOffset, m2: PayloadAndOffset) => if (m1.offset > m2.offset) m1 else m2

  val kafkaPartitions: List[Integer] = client.getPartitions().asScala.toList

  protected def getPartitions: Array[Partition] = {
    var index = -1
    kafkaPartitions.map { partition =>
      index += 1
      new KafkaSplit(id, index, partition)
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
    val kafkaSplit = split.asInstanceOf[KafkaSplit]
    val partition = kafkaSplit.partition
    val (startOffset, stopOffset) = client.getOffsets(partition).asScala.head
    val fetcher = client.iterator(partition, startOffset, stopOffset)
    val keySerdeInstance = keySerde
    val valueSerdeInstance = valueSerde

    context.addTaskCompletionListener { _ =>
      try {
        keySerdeInstance.close()
      } finally try {
        valueSerdeInstance.close()
      } finally {
        client.release(fetcher)
      }
    }

    val rawMessages = fetcher.asScala.map(x => (x.key, x.payloadAndOffset))

    val iterator = if (!compacted) rawMessages else {
      val spillMap = new ExternalAppendOnlyMap[ByteKey, PayloadAndOffset, PayloadAndOffset]((v) => v, compactor, compactor)
      spillMap.insertAll(rawMessages)
      spillMap.iterator
    }

    iterator.map { case (key, payloadAndOffset) =>
      (keySerdeInstance.fromBytes(key.bytes), valueSerdeInstance.fromBytes(payloadAndOffset.bytes))
    }.collect {
      case (k: K, v: V) => (k, v)
    }
  }

  def update(data: RDD[(K,V)]): Unit = {

    val produced = new LongAccumulator
    context.register(produced)

    def updatePartition(context: TaskContext, partition: Iterator[(K, V)]) {
      //TODO #17 hardcoded partitioner should be provided by the API instead
      val partitioner = new ObjectHashPartitioner
      val keySerdeInstance = keySerde
      val valueSerdeInstance = valueSerde

      val iterator = partition.map { case (k, v) =>
        val partition = partitioner.partition(k, kafkaPartitions.length)
        val payloadAndOffset = new PayloadAndOffset(partition.toLong, valueSerdeInstance.toBytes(v))
        new KeyPayloadAndOffset(new ByteKey(keySerdeInstance.toBytes(k)), payloadAndOffset)
      }.asJava

      val checker = new java.util.function.Function[java.lang.Long, java.lang.Boolean] {
        override def apply(partialCount: java.lang.Long): java.lang.Boolean = {
          produced.add(partialCount)
          !context.isInterrupted()
        }
      }

      client.publish(iterator, checker)
    }

    log.info(s"Producing into ${client} ...")

    context.runJob(data, updatePartition _)

    log.info(s"Produced ${produced.value} messages into ${client}")
  }
}
