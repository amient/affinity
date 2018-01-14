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

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import io.amient.affinity.core.ObjectHashPartitioner
import io.amient.affinity.core.serde.AbstractSerde
import io.amient.affinity.core.storage.EventTime
import io.amient.affinity.stream._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.util.collection.ExternalAppendOnlyMap
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

class CompactRDD[K: ClassTag, V: ClassTag](sc: SparkContext,
                                           bootstrapServer: String,
                                           keySerde: => AbstractSerde[_ >: K],
                                           valueSerde: => AbstractSerde[_ >: V],
                                           topic: String,
                                           compacted: Boolean = false) extends RDD[(K, V)](sc, Nil) {

  def this(sc: SparkContext, bootstrapServer: String, serde: => AbstractSerde[Any], topic: String, compacted: Boolean) =
    this(sc, bootstrapServer, serde, serde, topic, compacted)

  type ByteRecord = Record[Array[Byte], Array[Byte]]

  val compactor = (m1: ByteRecord, m2: ByteRecord) => if (m1.timestamp > m2.timestamp) m1 else m2

  def config: Config = ConfigFactory.empty()
    .withValue("bootstrap.servers", ConfigValueFactory.fromAnyRef(bootstrapServer))

  protected def getPartitions: Array[Partition] = {
    val consumer = ManagedStream.bindNewInstance(config)
    try {
      (0 until consumer.getNumPartitions(topic)).map { p =>
        new Partition {
          override def index = p
        }
      }.toArray
    } finally {
      consumer.close()
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
    val consumer = ManagedStream.bindNewInstance(config)
    val keySerdeInstance = keySerde
    val valueSerdeInstance = valueSerde
    context.addTaskCompletionListener { _ =>
      try {
        keySerdeInstance.close()
      } finally try {
        valueSerdeInstance.close()
      } finally {
        consumer.close()
      }
    }

    consumer.subscribe(topic, split.index)

    val rawRecords: Iterator[(ByteKey, ByteRecord)] = consumer.iterator()
      .map((record: ByteRecord) => (new ByteKey(record.key), record))

    val compactedRecords = if (!compacted) rawRecords else {
      val spillMap = new ExternalAppendOnlyMap[ByteKey, ByteRecord, ByteRecord]((v) => v, compactor, compactor)
      spillMap.insertAll(rawRecords)
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
      val stream = ManagedStream.bindNewInstance(config)
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
          val partition = partitioner.partition(k, getPartitions.length)
          val record = new Record(keySerdeInstance.toBytes(k), valueSerdeInstance.toBytes(v), ts)
          new PartitionedRecord(partition, record)
        }

        val checker = new java.util.function.Function[java.lang.Long, java.lang.Boolean] {
          override def apply(partialCount: java.lang.Long): java.lang.Boolean = {
            produced.add(partialCount)
            !context.isInterrupted()
          }
        }

        stream.publish(topic, iterator, checker)
        stream.close()
      } finally try {
        keySerdeInstance.close()
      } finally {
        valueSerdeInstance.close()
      }
    }

    log.info(s"Producing into $topic ...")

    context.runJob(data, updatePartition _)

    log.info(s"Produced ${produced.value} messages into $topic")
  }

}
