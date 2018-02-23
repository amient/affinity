/*
 * Copyright 2016-2018 Michal Harish, michal.harish@gmail.com
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

package io.amient.affinity.spark

import io.amient.affinity.core.serde.AbstractSerde
import io.amient.affinity.core.storage.{LogStorage, Record}
import io.amient.affinity.core.util.{EventTime, TimeRange}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkContext, TaskContext}

import scala.reflect.ClassTag

object CompactRDD {

  def apply[K: ClassTag, V: ClassTag](serdeBinder: => AbstractSerde[Any],
                                      storageBinder: => LogStorage[_],
                                      range: TimeRange,
                                      compacted: Boolean)(implicit sc: SparkContext): RDD[(K, V)] = {
    apply[K,V](serdeBinder, serdeBinder, storageBinder, range, compacted)
  }

  def apply[K: ClassTag, V: ClassTag](serdeBinder: => AbstractSerde[Any],
                                      storageBinder: => LogStorage[_],
                                      compacted: Boolean)(implicit sc: SparkContext): RDD[(K, V)] = {
    apply[K,V](serdeBinder, serdeBinder, storageBinder, TimeRange.UNBOUNDED, compacted)
  }

  def apply[K: ClassTag, V: ClassTag](serdeBinder: => AbstractSerde[Any],
                                      storageBinder: => LogStorage[_],
                                      range: TimeRange)(implicit sc: SparkContext): RDD[(K, V)] = {
    apply[K,V](serdeBinder, serdeBinder, storageBinder, range, compacted = true)
  }

  def apply[K: ClassTag, V: ClassTag](serdeBinder: => AbstractSerde[Any],
                                      storageBinder: => LogStorage[_])(implicit sc: SparkContext): RDD[(K, V)] = {
    apply[K,V](serdeBinder, serdeBinder, storageBinder, TimeRange.UNBOUNDED, compacted = true)
  }

  /**
    * Map underlying binary stream to typed RDD, either fully or from the given time range
    * @param keySerdeBinder
    * @param valueSerdeBinder
    * @param storageBinder
    * @param range
    * @param compacted
    * @param sc
    * @tparam K
    * @tparam V
    * @return RDD[(K,V)]
    */
  def apply[K: ClassTag, V: ClassTag](keySerdeBinder: => AbstractSerde[_ >: K],
                                      valueSerdeBinder: => AbstractSerde[_ >: V],
                                      storageBinder: => LogStorage[_],
                                      range: TimeRange,
                                      compacted: Boolean)(implicit sc: SparkContext): RDD[(K, V)] = {
    new BinaryCompactRDD(sc, storageBinder, range, compacted).mapPartitions { partition =>
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
    * append data into binary stream
    *
    * @param serdeBinder
    * @param storageBinder
    * @param data
    * @param sc
    * @tparam K
    * @tparam V
    */
  def apply[K: ClassTag, V: ClassTag](serdeBinder: => AbstractSerde[Any],
                                      storageBinder: => LogStorage[_],
                                      data: RDD[(K,V)])(implicit sc: SparkContext): Unit = {
    apply[K,V](serdeBinder, serdeBinder, storageBinder, data)
  }

  /**
    * append data into binary stream
    * @param keySerdeBinder
    * @param valueSerdeBinder
    * @param storageBinder
    * @param data
    * @param sc
    * @tparam K
    * @tparam V
    */
  def apply[K: ClassTag, V: ClassTag](keySerdeBinder: => AbstractSerde[_ >: K],
                                      valueSerdeBinder: => AbstractSerde[_ >: V],
                                      storageBinder: => LogStorage[_],
                                      data: RDD[(K,V)])(implicit sc: SparkContext): Unit = {
    val produced = new LongAccumulator

    sc.register(produced)

    def updatePartition(context: TaskContext, partition: Iterator[(K, V)]) {
      val storage = storageBinder
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
          new Record(serializedKey, serializedValue, ts)
        }
        iterator.foreach { record =>
          storage.append(record)
          produced.add(1)
        }
        storage.flush
        storage.close()
      } finally try {
        keySerde.close()
      } finally {
        valueSerde.close()
      }
    }

    sc.runJob(data, updatePartition _)

  }
}
