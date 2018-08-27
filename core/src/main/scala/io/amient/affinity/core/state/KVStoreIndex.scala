package io.amient.affinity.core.state

import java.nio.ByteBuffer
import java.util.Optional
import java.util.concurrent.{ConcurrentHashMap, TimeoutException}

import io.amient.affinity.core.serde.AbstractSerde
import io.amient.affinity.core.storage.{MemStore, Record}
import io.amient.affinity.core.util.EventTime

import scala.util.control.Breaks.{break, breakable}

class KVStoreIndex[K, V](identifier: String,
                         memstore: MemStore,
                         keySerde: AbstractSerde[K],
                         valueSerde: AbstractSerde[V],
                         ttlMs: Long,
                         lockTimeoutMs: Long) {

  def option[T](opt: Optional[T]): Option[T] = if (opt.isPresent) Some(opt.get()) else None

  def apply(key: K): Option[V] = apply(ByteBuffer.wrap(keySerde.toBytes(key)))

  private def apply(key: ByteBuffer): Option[V] = {
    //    val timerContext = readsMeter.markStart()
    try {
      for (
        cell: ByteBuffer <- option(memstore(key));
        byteRecord: Record[Array[Byte], Array[Byte]] <- option(memstore.unwrap(key, cell, ttlMs))
      ) yield {
        val result = valueSerde.fromBytes(byteRecord.value)
        //        readsMeter.markSuccess(timerContext)
        result
      }
    } catch {
      case e: Throwable =>
        //        readsMeter.markFailure(timerContext)
        throw e
    }
  }

  def numKeys: Long = memstore.numKeys()

  def getStats: String = {
    s"$identifier\n===================================================================\n" +
      s"MemStore[${memstore.getClass.getSimpleName}]\n${memstore.getStats}\n\n"
  }


  def update(key: K)(f: Option[V] => V): V = {
    val k = keySerde.toBytes(key)
    lock(key)
    try {
      val currentValue: Option[V] = apply(ByteBuffer.wrap(k))
      val updatedValue: V = f(currentValue)
      if (currentValue != Some(updatedValue)) put(k, updatedValue)
      updatedValue
    } finally {
      unlock(key)
    }
  }

  private def put(key: Array[Byte], value: V): Option[V] = {
    val nowMs = EventTime.unix
    val recordTimestamp = value match {
      case e: EventTime => e.eventTimeUnix()
      case _ => nowMs
    }
    if (ttlMs > 0 && recordTimestamp + ttlMs < nowMs) {
      delete(key)
    } else {
      //      val timerContext = writesMeter.markStart()
      try {
        val valueBytes = valueSerde.toBytes(value)
        memstore.put(ByteBuffer.wrap(key), memstore.wrap(valueBytes, recordTimestamp))
        Some(value)
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

  /**
    * row locking functionality
    */
  private class RowLock extends AnyRef

  private val locks = new ConcurrentHashMap[Any, RowLock]

  def lock(scope: Any): Unit = {
    val lock = new RowLock()
    breakable {
      val start = System.currentTimeMillis
      do {
        val existingLock = locks.putIfAbsent(scope, lock)
        if (existingLock == null) break else existingLock.synchronized {
          existingLock.wait(lockTimeoutMs)
          val waitedMs = System.currentTimeMillis - start
          if (waitedMs >= lockTimeoutMs) {
            throw new TimeoutException(s"Could not acquire lock for $scope in $lockTimeoutMs ms")
          }
        }
      } while (true)
    }
  }

  def unlock(scope: Any): Unit = {
    val lock = locks.remove(scope)
    lock.synchronized {
      lock.notifyAll()
    }
  }
}
