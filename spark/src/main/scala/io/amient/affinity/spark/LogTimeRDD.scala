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
