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

import io.amient.affinity.core.util.TimeRange
import io.amient.affinity.stream._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.ExternalAppendOnlyMap
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.JavaConversions._

class BinaryCompactRDD(sc: SparkContext, streamBinder: => BinaryStream, range: TimeRange, compacted: Boolean)
  extends RDD[(ByteKey, BinaryRecord)](sc, Nil) {

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

  override def compute(split: Partition, context: TaskContext): Iterator[(ByteKey, BinaryRecord)] = {
    val stream = streamBinder
    context.addTaskCompletionListener { _ =>
      stream.close()
    }

    stream.scan(split.index, range)

    val binaryRecords: Iterator[BinaryRecord] = stream.iterator()
    val kvRecords: Iterator[(ByteKey, BinaryRecord)] = binaryRecords.map(record => (new ByteKey(record.key), record))

    val compactedRecords: Iterator[(ByteKey, BinaryRecord)] = if (!compacted) kvRecords else {
      val spillMap = new ExternalAppendOnlyMap[ByteKey, BinaryRecord, BinaryRecord]((v) => v, compactor, compactor)
      spillMap.insertAll(kvRecords)
      spillMap.iterator
    }
    compactedRecords
  }

}
