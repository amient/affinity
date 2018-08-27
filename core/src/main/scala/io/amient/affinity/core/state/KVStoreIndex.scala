package io.amient.affinity.core.state

import java.nio.ByteBuffer
import java.util.Optional

import io.amient.affinity.core.serde.AbstractSerde
import io.amient.affinity.core.storage.MemStore
import io.amient.affinity.core.util.{ByteUtils, CloseableIterator, EventTime, TimeRange}

import scala.collection.JavaConverters._

class KVStoreIndex[K, V](identifier: String,
                         memstore: MemStore,
                         keySerde: AbstractSerde[K],
                         valueSerde: AbstractSerde[V],
                         ttlMs: Long) {

  def option[T](opt: Optional[T]): Option[T] = if (opt.isPresent) Some(opt.get()) else None

  def apply(key: K, range: TimeRange = TimeRange.UNBOUNDED): CloseableIterator[V] = new CloseableIterator[V] {
    val bytePrefix: ByteBuffer = ByteBuffer.wrap(ByteUtils.intValue(key.hashCode()))
    val underlying = memstore.iterator(bytePrefix)
    val mapped = underlying.asScala.flatMap { entry =>
      option(memstore.unwrap(entry.getKey(), entry.getValue, ttlMs))
        .filter(byteRecord => range.contains(byteRecord.timestamp))
        .filter(byteRecord => keySerde.fromBytes(byteRecord.value) == key)
        .map { byteRecord =>
          val resultKey = new Array[Byte](byteRecord.key.length - 4)
          ByteUtils.copy(byteRecord.key, 4, resultKey, 0, resultKey.length)
          valueSerde.fromBytes(resultKey)
        }
    }

    override def next(): V = mapped.next()

    override def hasNext: Boolean = mapped.hasNext

    override def close(): Unit = underlying.close()
  }

  def numKeys: Long = memstore.numKeys()

  def getStats: String = {
    s"$identifier\n===================================================================\n" +
      s"MemStore[${memstore.getClass.getSimpleName}]\n${memstore.getStats}\n\n"
  }

  def put(k: K, value: V, timestamp: Long): Unit = {

    val bytePrefix = valueSerde.toBytes(value)
    val indexKey = new Array[Byte](4 + bytePrefix.length)
    ByteUtils.putIntValue(k.hashCode(), indexKey, 0)
    ByteUtils.copy(bytePrefix, 0, indexKey, 4, bytePrefix.length)

    val nowMs = EventTime.unix
    if (ttlMs > 0 && timestamp + ttlMs < nowMs) {
      delete(indexKey)
    } else {
      //      val timerContext = writesMeter.markStart()
      try {
        memstore.put(ByteBuffer.wrap(indexKey), memstore.wrap(keySerde.toBytes(k), timestamp))
      } catch {
        case e: Throwable =>
          //          writesMeter.markFailure(timerContext)
          throw e
      }
    }
  }

  private def delete(key: Array[Byte]): Option[V] = {
    //    val timerContext = writesMeter.markStart()
    try {
      memstore.remove(ByteBuffer.wrap(key))
      //      writesMeter.markSuccess(timerContext)
      None
    } catch {
      case e: Throwable =>
        //        writesMeter.markFailure(timerContext)
        throw e
    }

  }

  def close(): Unit = {
    memstore.close()
    //    metrics.remove(s"state.$identifier.keys")
  }

}
