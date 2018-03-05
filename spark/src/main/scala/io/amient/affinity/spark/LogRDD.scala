package io.amient.affinity.spark

import io.amient.affinity.core.storage.{LogStorage, Record}
import io.amient.affinity.core.util.TimeRange
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import scala.collection.JavaConverters._

class LogRDD(sc: SparkContext, storageBinder: => LogStorage[_], range: TimeRange)
  extends RDD[Record[Array[Byte], Array[Byte]]](sc, Nil) {

  type R = Record[Array[Byte], Array[Byte]]

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

  override def compute(split: Partition, context: TaskContext): Iterator[Record[Array[Byte], Array[Byte]]] = {
    val storage = storageBinder
    storage.reset(split.index, range)
    context.addTaskCompletionListener(_ =>  storage.close)
    storage.boundedIterator().asScala
  }

}

object LogRDD {
  /**
    * Create a 2-dimensional RDD which projects event-time and processing-time of the given stream log
    *
    * @param storageBinder binder for the log
    * @param range time range to consider
    * @param sc spark context
    * @return RDD[(event-time:Long, processing-time: Long)]
    */
  def timelog(storageBinder: => LogStorage[_], range: TimeRange)(implicit sc: SparkContext): RDD[(Long, Long)] = {
    new LogRDD(sc, storageBinder, range).mapPartitions { partition =>
      var processTime = 0L
      partition.map(record => (record.timestamp, {
        processTime += 1;
        processTime
      }))
    }
  }
}

