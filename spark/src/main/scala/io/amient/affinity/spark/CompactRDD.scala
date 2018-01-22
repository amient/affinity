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

import io.amient.affinity.core.Murmur2Partitioner
import io.amient.affinity.core.serde.AbstractSerde
import io.amient.affinity.core.util.EventTime
import io.amient.affinity.stream._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.util.collection.ExternalAppendOnlyMap
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

class CompactRDD[K: ClassTag, V: ClassTag](sc: SparkContext,
                                           keySerde: => AbstractSerde[_ >: K],
                                           valueSerde: => AbstractSerde[_ >: V],
                                           streamBinder: => BinaryStream,
                                           compacted: Boolean) extends RDD[(K, V)](sc, Nil) {

  def this(sc: SparkContext, serde: => AbstractSerde[Any], streamBinder: => BinaryStream, compacted: Boolean = true) =
    this(sc, serde, serde, streamBinder, compacted)

  val compactor = (m1: BinaryRecord, m2: BinaryRecord) => if (m1.timestamp > m2.timestamp) m1 else m2

  protected def getPartitions: Array[Partition] = {
    val stream = streamBinder
    try {
      (0 until stream.getNumPartitions()).map { p =>
        new Partition {
          override def index = p
        }
      }.toArray
    } finally {
      stream.close()
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
    val stream = streamBinder
    val keySerdeInstance = keySerde
    val valueSerdeInstance = valueSerde
    context.addTaskCompletionListener { _ =>
      try {
        keySerdeInstance.close()
      } finally try {
        valueSerdeInstance.close()
      } finally {
        stream.close()
      }
    }

    stream.subscribe(split.index)

    val binaryRecords: Iterator[BinaryRecord] = stream.iterator()
    val kvRecords: Iterator[(ByteKey, BinaryRecord)] = binaryRecords.map(record => (new ByteKey(record.key), record))

    val compactedRecords = if (!compacted) kvRecords else {
      val spillMap = new ExternalAppendOnlyMap[ByteKey, BinaryRecord, BinaryRecord]((v) => v, compactor, compactor)
      spillMap.insertAll(kvRecords)
      spillMap.iterator
    }

    compactedRecords.map { case (key, record) =>
      (keySerdeInstance.fromBytes(key.bytes), valueSerdeInstance.fromBytes(record.value))
    }.collect {
      case (k: K, v: V) => (k, v)
    }
  }

  def update(data: RDD[(K,V)]): Unit = {

    val produced = new LongAccumulator
    context.register(produced)

    def updatePartition(context: TaskContext, partition: Iterator[(K, V)]) {
      val stream = streamBinder
      val keySerdeInstance = keySerde
      val valueSerdeInstance = valueSerde

      try {
        val iterator = partition.map { case (k, v) =>
          val ts = v match {
            case e: EventTime => e.eventTimeUnix()
            case _ => System.currentTimeMillis()
          }
          val serializedKey = keySerdeInstance.toBytes(k)
          val serializedValue = valueSerdeInstance.toBytes(v)
          new BinaryRecord(serializedKey, serializedValue, ts)
        }

        produced.add(stream.publish(iterator))
        stream.close()
      } finally try {
        keySerdeInstance.close()
      } finally {
        valueSerdeInstance.close()
      }
    }

    context.runJob(data, updatePartition _)

    log.info(s"Produced ${produced.value} messages")
  }

}
