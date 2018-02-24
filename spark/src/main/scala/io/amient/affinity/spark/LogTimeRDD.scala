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

import io.amient.affinity.core.storage.LogStorage
import io.amient.affinity.core.util.TimeRange
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object LogTimeRDD {

  /**
    * Create an RDD on top of binary stream which plots event-time and relative processing time onto
    * a 2-dimenstional dataset
    *
    * @param storageBinder binder for the
    * @param range
    * @param sc
    * @return RDD[(event-time:Long, processing-time: Long)]
    */
  def apply(storageBinder: => LogStorage[_], range: TimeRange)(implicit sc: SparkContext): RDD[(Long, Long)] = {
    new BinaryCompactRDD(sc, storageBinder, range, compacted = false).mapPartitions {
      partition =>
        var processTime = 0L
        partition.map(record => (record._2.timestamp, {
          processTime += 1; processTime
        }))
    }
  }

}
