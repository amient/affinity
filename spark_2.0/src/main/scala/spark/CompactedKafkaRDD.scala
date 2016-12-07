/**
  * HttpCompactedRDD
  * Copyright (C) 2015 Michal Harish
  * <p/>
  * This program is free software: you can redistribute it and/or modify
  * it under the terms of the GNU General Public License as published by
  * the Free Software Foundation, either version 3 of the License, or
  * (at your option) any later version.
  * <p/>
  * This program is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  * GNU General Public License for more details.
  * <p/>
  * You should have received a copy of the GNU General Public License
  * along with this program.  If not, see <http://www.gnu.org/licenses/>.
  */

package io.amient.util.spark

import kafka.api.OffsetRequest
import org.apache.spark.util.collection.ExternalAppendOnlyMap
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

abstract class CompactedKafkaRDD[K: ClassTag, V: ClassTag](
      sc: SparkContext,
      topic: String,
      kafkaConfig: KafkaConfig,
      sinceTimeMs: Long = OffsetRequest.EarliestTime
      ) extends KafkaRDD[K, V](sc, topic, kafkaConfig, extraParallelism = -1, sinceTimeMs, selectPartition = -1) {

  override def computeKafka(split: Partition, context: TaskContext): Iterator[(ByteKey, PayloadAndOffset)] = {
    val records: Iterator[(ByteKey, PayloadAndOffset)] = super.computeKafka(split, context)
    val compactor = (m1: PayloadAndOffset, m2: PayloadAndOffset) => if (m1.offset > m2.offset) m1 else m2
    val spillMap = new ExternalAppendOnlyMap[ByteKey, PayloadAndOffset, PayloadAndOffset]((v) => v, compactor, compactor)
    spillMap.insertAll(records)
    spillMap.iterator
  }

}
