package io.amient.affinity.core.state

import java.nio.ByteBuffer
import java.util.Optional

import io.amient.affinity.core.serde.AbstractSerde
import io.amient.affinity.core.storage.MemStore
import io.amient.affinity.core.util.{ByteUtils, EventTime, TimeRange}

import scala.collection.JavaConverters._

class KVStoreIndex[K, V](identifier: String,
                         memstore: MemStore,
                         keySerde: AbstractSerde[K],
                         valueSerde: AbstractSerde[V],
                         ttlMs: Long) {

  def option[T](opt: Optional[T]): Option[T] = if (opt.isPresent) Some(opt.get()) else None

  def apply[T](key: K, range: TimeRange = TimeRange.UNBOUNDED)(f: Iterator[V] => T): T = {
    val bytePrefix: ByteBuffer = ByteBuffer.wrap(ByteUtils.intValue(key.hashCode()))
    val underlying = memstore.iterator(bytePrefix)
    try {
      f(underlying.asScala.flatMap { entry =>
        option(memstore.unwrap(entry.getKey(), entry.getValue, ttlMs))
          .filter(byteRecord => range.contains(byteRecord.timestamp))
          .filter(byteRecord => keySerde.fromBytes(byteRecord.value) == key)
          .map { byteRecord =>
            val resultKey = new Array[Byte](byteRecord.key.length - 4)
            ByteUtils.copy(byteRecord.key, 4, resultKey, 0, resultKey.length)
            valueSerde.fromBytes(resultKey)
          }
      })
    } finally {
      underlying.close()
    }
  }

  def numKeys: Long = memstore.numKeys()

  def getStats: String = {
    s"$identifier\n===================================================================\n" +
      s"MemStore[${memstore.getClass.getSimpleName}]\n${memstore.getStats}\n\n"
  }

  def put(k: K, value: V, timestamp: Long, tombstone: Boolean = false): Unit = {

    val bytePrefix = valueSerde.toBytes(value)
    val indexKey = new Array[Byte](4 + bytePrefix.length)
    ByteUtils.putIntValue(k.hashCode(), indexKey, 0)
    ByteUtils.copy(bytePrefix, 0, indexKey, 4, bytePrefix.length)

    //      val timerContext = writesMeter.markStart()
    try {
      if (tombstone || (ttlMs > 0 && timestamp + ttlMs < EventTime.unix)) {
        memstore.remove(ByteBuffer.wrap(indexKey))
      } else {
        memstore.put(ByteBuffer.wrap(indexKey), memstore.wrap(keySerde.toBytes(k), timestamp))
      }
      //      writesMeter.markSuccess(timerContext)
    } catch {
      case e: Throwable =>
        //          writesMeter.markFailure(timerContext)
        throw e
    }
  }

  def close(): Unit = {
    memstore.close()
    //    metrics.remove(s"state.$identifier.keys")
  }

}
