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

import io.amient.affinity.core.ObjectHashPartitioner
import io.amient.affinity.core.serde.AbstractSerde
import io.amient.affinity.core.storage.EventTime
import io.amient.affinity.spark.StreamSplit
import io.amient.affinity.stream.{ByteKey, PartitionedRecord, Record, StreamClient}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.util.collection.ExternalAppendOnlyMap
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class CompactRDD[K: ClassTag, V: ClassTag](sc: SparkContext,
                                           client: StreamClient,
                                           keySerde: => AbstractSerde[_ >: K],
                                           valueSerde: => AbstractSerde[_ >: V],
                                           compacted: Boolean = false) extends RDD[(K, V)](sc, Nil) {

  def this(sc: SparkContext, client: StreamClient, serde: => AbstractSerde[Any], compacted: Boolean) =
    this(sc, client, serde, serde, compacted)


  val compactor = (m1: Record[Array[Byte], Array[Byte]], m2: Record[Array[Byte], Array[Byte]]) =>
    if (m1.timestamp > m2.timestamp) m1 else m2

  val streamPartitions: List[Integer] = client.getPartitions().asScala.toList

  protected def getPartitions: Array[Partition] = {
    var index = -1
    streamPartitions.map { partition =>
      index += 1
      new StreamSplit(id, index, partition)
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
    val streamSplit = split.asInstanceOf[StreamSplit]
    val partition = streamSplit.partition
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

    val rawMessages: Iterator[(ByteKey, Record[Array[Byte], Array[Byte]])] = {
      fetcher.asScala.map((record: Record[Array[Byte], Array[Byte]]) => (new ByteKey(record.key), record))
    }

    val iterator = if (!compacted) rawMessages else {
      val spillMap = new ExternalAppendOnlyMap[ByteKey, Record[Array[Byte], Array[Byte]], Record[Array[Byte], Array[Byte]]]((v) => v, compactor, compactor)
      spillMap.insertAll(rawMessages)
      spillMap.iterator
    }

    iterator.map { case (key, record) =>
      (keySerdeInstance.fromBytes(key.bytes), valueSerdeInstance.fromBytes(record.value))
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

      try {
        val iterator = partition.map { case (k, v) =>
          val ts = v match {
            case e: EventTime => e.eventTimeUtc()
            case _ => System.currentTimeMillis()
          }
          val partition = partitioner.partition(k, streamPartitions.length)
          val record = new Record(keySerdeInstance.toBytes(k), valueSerdeInstance.toBytes(v), ts)
          new PartitionedRecord(partition, record)
        }.asJava

        val checker = new java.util.function.Function[java.lang.Long, java.lang.Boolean] {
          override def apply(partialCount: java.lang.Long): java.lang.Boolean = {
            produced.add(partialCount)
            !context.isInterrupted()
          }
        }

        client.publish(iterator, checker)
      } finally try {
        keySerdeInstance.close()
      } finally {
        valueSerdeInstance.close()
      }
    }

    log.info(s"Producing into ${client} ...")

    context.runJob(data, updatePartition _)

    log.info(s"Produced ${produced.value} messages into ${client}")
  }
}
