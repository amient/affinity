package io.amient.affinity.spark

import io.amient.affinity.core.serde.AbstractSerde
import io.amient.affinity.core.storage.{ByteKey, LogEntry, LogStorage, Record}
import io.amient.affinity.core.util.{EventTime, TimeRange}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.util.collection.ExternalAppendOnlyMap
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class LogRDD[POS <: Comparable[POS]] private(@transient private val sc: SparkContext,
                                             storageBinder: => LogStorage[POS], range: TimeRange, compacted: Boolean)
  extends RDD[(ByteKey, LogEntry[_])](sc, Nil) {

  def this(sc: SparkContext, storageBinder: => LogStorage[POS], range: TimeRange) {
    this(sc, storageBinder, range, false)
  }

  def this(sc: SparkContext, storageBinder: => LogStorage[POS]) {
    this(sc, storageBinder, TimeRange.UNBOUNDED)
  }

  /**
    * @return compacted LogRDD version of this LogRDD
    */
  def compact = if (compacted) this else new LogRDD[POS](sc, storageBinder, range, true)

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

  override def compute(split: Partition, context: TaskContext): Iterator[(ByteKey, LogEntry[_])] = {
    val storage: LogStorage[POS] = storageBinder
    storage.reset(split.index, range)
    context.addTaskCompletionListener(_ => storage.close)
    val compactor = (r1: LogEntry[POS], r2: LogEntry[POS]) => if (r1.timestamp > r2.timestamp) r1 else r2
    val logRecords = storage.boundedIterator().asScala.map(record => (new ByteKey(record.key), record))
    if (!compacted) logRecords else {
      val spillMap = new ExternalAppendOnlyMap[ByteKey, LogEntry[POS], LogEntry[POS]]((v) => v, compactor, compactor)
      spillMap.insertAll(logRecords)
      spillMap.iterator.filter { case (_, entry) => !storage.isTombstone(entry) }
    }
  }


  /**
    * Create a 2-dimensional RDD which projects event-time and processing-time of the given stream log
    *
    * @return RDD[(event-time:Long, processing-time: Long)]
    */
  def timelog: RDD[(Long, Long)] = {
    var processTime = 0L
    map { case (key, entry) => entry.timestamp -> {
      processTime += 1
      processTime
    }
    }
  }

  /**
    * transform the bianry LogRDD into RDD[(K,V)] using the give serdes
    * @param serdeBinder serde used for both keys and values
    * @tparam K Key type
    * @tparam V Value type
    */
  def present[K: ClassTag, V: ClassTag](serdeBinder: => AbstractSerde[Any]): RDD[(K, V)] = {
    present[K, V](serdeBinder, serdeBinder)
  }

  /**
    * transform the bianry LogRDD into RDD[(K,V)] using the give serdes
    *
    * @param keySerdeBinder serde for Key types
    * @param valueSerdeBinder serde for this LogRDD value type
    * @tparam K Key type of both rdds
    * @tparam V Value type of this rdd
    * @return RDD of deserialized keys and values
    */
  def present[K: ClassTag, V: ClassTag](keySerdeBinder: => AbstractSerde[_ >: K], valueSerdeBinder: => AbstractSerde[_ >: V]): RDD[(K, V)] = {
    mapPartitions { compactedRecords =>
      val keySerde = keySerdeBinder
      val valueSerde = valueSerdeBinder
      TaskContext.get.addTaskCompletionListener { _ =>
        try keySerde.close finally valueSerde.close
      }
      compactedRecords.map { case (key, record) =>
        (keySerde.fromBytes(key.bytes).asInstanceOf[K], valueSerde.fromBytes(record.value).asInstanceOf[V])
      }
    }
  }

  def join[K: ClassTag, V: ClassTag, X](serdeBinder: => AbstractSerde[Any], other: RDD[(K, X)]): RDD[(K, (V, X))] = {
    join[K, V, X](serdeBinder, serdeBinder, other)
  }

  /**
    * Serialization-optimized join:
    *   1. serialize key of the other table into a ByteKey
    *   2. join on ByteKey before deserializing value
    *   3. deserialize value on the reusult subset only
    *
    * @param keySerdeBinder   serde for Key types
    * @param valueSerdeBinder serde for this LogRDD value type
    * @param other            RDD to join on the right side
    * @tparam K Key type of both rdds
    * @tparam V Value type of this rdd
    * @tparam W Value type of the other rdd
    * @return joined pair rdd where the result value has the value from this rdd on the left
    *         and the value from the other rdd on the right
    */
  def join[K: ClassTag, V: ClassTag, W](keySerdeBinder: => AbstractSerde[_ >: K],
                                        valueSerdeBinder: => AbstractSerde[_ >: V],
                                        other: RDD[(K, W)]): RDD[(K, (V, W))] = {

    val otherWithByteKey: RDD[(ByteKey, (K, W))] = other.mapPartitions { partition =>
      val keySerde = keySerdeBinder
      TaskContext.get.addTaskCompletionListener(_ => keySerde.close)
      partition.map { case (k: K, x) => (new ByteKey(keySerde.toBytes(k)), (k, x)) }
    }

    val inverseJoin: RDD[((K, W), LogEntry[_])] = otherWithByteKey.join(this).values

    inverseJoin.mapPartitions { partition =>
      val valueSerde = valueSerdeBinder
      TaskContext.get.addTaskCompletionListener(_ => valueSerde.close)
      partition.map { case ((k, x), r) => (k, (valueSerde.fromBytes(r.value).asInstanceOf[V], x)) }
    }
  }

}


object LogRDD {

  /**
    * Map an RDD to the underlying binary log stream
    *
    * @param storageBinder binding for the log storage
    * @param sc spark context
    * @tparam POS type of the log position
    * @return LogRDD
    */
  def apply[POS <: Comparable[POS]](storageBinder: => LogStorage[POS], range: TimeRange = TimeRange.UNBOUNDED)
                                   (implicit sc: SparkContext): LogRDD[POS] = {
    new LogRDD[POS](sc, storageBinder, range, compacted = false)
  }

  /**
    * append a set key-value pairs to the log storage
    *
    * @param serdeBinder serde used for both keys and values
    * @param storageBinder binding for the log storage
    * @param data rdd containing the key-value pairs to be appended
    * @param sc spark context
    * @tparam K key type
    * @tparam V value type
    */
  def append[K: ClassTag, V: ClassTag](serdeBinder: => AbstractSerde[Any],
                                       storageBinder: => LogStorage[_],
                                       data: RDD[(K, V)])(implicit sc: SparkContext): Unit = {
    append[K, V](serdeBinder, serdeBinder, storageBinder, data)
  }

  /**
    * append a set key-value pairs to the log storage
    *
    * @param keySerdeBinder serde used for keys
    * @param valueSerdeBinder serde used for values
    * @param storageBinder binding for the log storage
    * @param data rdd containing the key-value pairs to be appended
    * @param sc spark context
    * @tparam K key type
    * @tparam V value type
    */
  def append[K: ClassTag, V: ClassTag](keySerdeBinder: => AbstractSerde[_ >: K],
                                       valueSerdeBinder: => AbstractSerde[_ >: V],
                                       storageBinder: => LogStorage[_],
                                       data: RDD[(K, V)])(implicit sc: SparkContext): Unit = {
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
