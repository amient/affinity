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
import io.amient.affinity.core.util.{EventTime, TimeRange}
import io.amient.affinity.spark.BinaryCompactRDD
import io.amient.affinity.stream._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkContext, TaskContext}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

object CompactRDD {

  def apply[K: ClassTag, V: ClassTag](serdeBinder: => AbstractSerde[Any],
                                      streamBinder: => BinaryStream,
                                      compacted: Boolean)(implicit sc: SparkContext): RDD[(K, V)] = {
    apply[K,V](serdeBinder, serdeBinder, streamBinder, TimeRange.ALLTIME, compacted)
  }

  def apply[K: ClassTag, V: ClassTag](serdeBinder: => AbstractSerde[Any],
                                      streamBinder: => BinaryStream,
                                      range: TimeRange)(implicit sc: SparkContext): RDD[(K, V)] = {
    apply[K,V](serdeBinder, serdeBinder, streamBinder, range, compacted = true)
  }

  def apply[K: ClassTag, V: ClassTag](serdeBinder: => AbstractSerde[Any],
                                      streamBinder: => BinaryStream)(implicit sc: SparkContext): RDD[(K, V)] = {
    apply[K,V](serdeBinder, serdeBinder, streamBinder, TimeRange.ALLTIME, compacted = true)
  }

  /**
    * Map underlying binary stream to typed RDD, either fully or from the given time range
    * @param keySerdeBinder
    * @param valueSerdeBinder
    * @param streamBinder
    * @param range
    * @param compacted
    * @param sc
    * @tparam K
    * @tparam V
    * @return RDD[(K,V)]
    */
  def apply[K: ClassTag, V: ClassTag](keySerdeBinder: => AbstractSerde[_ >: K],
                                      valueSerdeBinder: => AbstractSerde[_ >: V],
                                      streamBinder: => BinaryStream,
                                      range: TimeRange,
                                      compacted: Boolean)(implicit sc: SparkContext): RDD[(K, V)] = {
    new BinaryCompactRDD(sc, streamBinder, range, compacted).mapPartitions { partition =>
      val keySerde = keySerdeBinder
      val valueSerde = valueSerdeBinder
      TaskContext.get.addTaskCompletionListener { _ =>
        try keySerde.close finally valueSerde.close
      }
      partition.flatMap { case (key, record) =>
        (keySerde.fromBytes(key.bytes), valueSerde.fromBytes(record.value)) match {
          case (k: K, v: V) => Some((k, v))
          case _ => None
        }
      }
    }
  }

  /**
    * publish data into binary stream
    *
    * @param serdeBinder
    * @param streamBinder
    * @param data
    * @param sc
    * @tparam K
    * @tparam V
    */
  def apply[K: ClassTag, V: ClassTag](serdeBinder: => AbstractSerde[Any],
                                      streamBinder: => BinaryStream,
                                      data: RDD[(K,V)])(implicit sc: SparkContext): Unit = {
    apply[K,V](serdeBinder, serdeBinder, streamBinder, data)
  }

  /**
    * publish data into binary stream
    * @param keySerdeBinder
    * @param valueSerdeBinder
    * @param streamBinder
    * @param data
    * @param sc
    * @tparam K
    * @tparam V
    */
  def apply[K: ClassTag, V: ClassTag](keySerdeBinder: => AbstractSerde[_ >: K],
                                      valueSerdeBinder: => AbstractSerde[_ >: V],
                                      streamBinder: => BinaryStream,
                                      data: RDD[(K,V)])(implicit sc: SparkContext): Unit = {
    val produced = new LongAccumulator

    sc.register(produced)

    def updatePartition(context: TaskContext, partition: Iterator[(K, V)]) {
      val stream = streamBinder
      val keySerde = keySerdeBinder
      val valueSerde = valueSerdeBinder

      try {
        val iterator = partition.map { case (k, v) =>
          val ts = v match {
            case e: EventTime => e.eventTimeUnix()
            case _ => System.currentTimeMillis()
          }
          val serializedKey = keySerde.toBytes(k)
          val serializedValue = valueSerde.toBytes(v)
          new BinaryRecord(serializedKey, serializedValue, ts)
        }
        produced.add(stream.publish(iterator))
        stream.flush
        stream.close()
      } finally try {
        keySerde.close()
      } finally {
        valueSerde.close()
      }
    }

    sc.runJob(data, updatePartition _)

  }
}
