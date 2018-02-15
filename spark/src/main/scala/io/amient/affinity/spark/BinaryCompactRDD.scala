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

  val compactor = (m1: BinaryRecordAndOffset, m2: BinaryRecordAndOffset) => if (m1.offset > m2.offset) m1 else m2

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

    val binaryRecords: Iterator[BinaryRecordAndOffset] = stream.iterator()
    val kvRecords: Iterator[(ByteKey, BinaryRecordAndOffset)] = binaryRecords.map(record => (new ByteKey(record.key), record))

    val compactedRecords: Iterator[(ByteKey, BinaryRecordAndOffset)] = if (!compacted) kvRecords else {
      val spillMap = new ExternalAppendOnlyMap[ByteKey, BinaryRecordAndOffset, BinaryRecordAndOffset]((v) => v, compactor, compactor)
      spillMap.insertAll(kvRecords)
      spillMap.iterator
    }
    compactedRecords
  }

}
