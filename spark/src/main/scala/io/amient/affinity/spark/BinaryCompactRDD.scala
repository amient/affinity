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

package io.amient.affinity.spark

import io.amient.affinity.core.storage.{ByteKey, LogStorage, Record}
import io.amient.affinity.core.util.TimeRange
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.ExternalAppendOnlyMap
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.JavaConversions._

class BinaryCompactRDD(sc: SparkContext, storageBinder: => LogStorage[_], range: TimeRange, compacted: Boolean)
  extends RDD[(ByteKey, Record[Array[Byte], Array[Byte]])](sc, Nil) {

  type R = Record[Array[Byte], Array[Byte]]

  val compactor = (r1: R, r2: R) => if (r1.timestamp > r2.timestamp) r1 else r2

  protected def getPartitions: Array[Partition] = {
    val stream = storageBinder
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

  override def compute(split: Partition, context: TaskContext): Iterator[(ByteKey, R)] = {
    val storage = storageBinder
    storage.reset(split.index, range)
    context.addTaskCompletionListener(_ =>  storage.close)

    val logRecords = storage.boundedIterator().map(record => (new ByteKey(record.key), record))
    val compactedRecords: Iterator[(ByteKey, R)] = if (!compacted) logRecords else {
      val spillMap = new ExternalAppendOnlyMap[ByteKey, R, R]((v) => v, compactor, compactor)
      spillMap.insertAll(logRecords)
      spillMap.iterator
    }
    compactedRecords
  }

}
