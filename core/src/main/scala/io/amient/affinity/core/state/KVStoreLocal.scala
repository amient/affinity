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

package io.amient.affinity.core.state

import java.lang
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, TimeoutException}
import java.util.{Observable, Observer, Optional}

import akka.actor.{ActorRef, ActorSystem, Props}
import com.codahale.metrics.{Gauge, MetricRegistry}
import com.typesafe.config.Config
import io.amient.affinity.Conf
import io.amient.affinity.avro.AvroSchemaRegistry
import io.amient.affinity.avro.record.{AvroRecord, AvroSerde}
import io.amient.affinity.core.actor.KeyValueMediator
import io.amient.affinity.core.serde.avro.AvroSerdeProxy
import io.amient.affinity.core.serde.{AbstractSerde, Serde}
import io.amient.affinity.core.storage._
import io.amient.affinity.core.util.{AffinityMetrics, CloseableIterator, EventTime, TimeRange}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.language.{existentials, implicitConversions}
import scala.reflect.ClassTag
import scala.util.control.Breaks._
import scala.util.control.NonFatal

object KVStoreLocal {

  def create[K: ClassTag, V: ClassTag](name: String,
                                       partition: Int,
                                       stateConf: StateConf,
                                       numPartitions: Int,
                                       system: ActorSystem): KVStoreLocal[K, V] = {
    val identifier = if (partition < 0) name else s"$name-$partition"
    val keySerde = Serde.of[K](system.settings.config)
    val valueSerde = Serde.of[V](system.settings.config)
    val keyClass = implicitly[ClassTag[K]].runtimeClass
    val kvstoreClass = stateConf.MemStore.Class()
    val kvstoreConstructor = kvstoreClass.getConstructor(classOf[String], classOf[StateConf], classOf[MetricRegistry])
    if (!stateConf.MemStore.DataDir.isDefined) {
      val nodeConf = Conf(system.settings.config).Affi.Node
      if (nodeConf.DataDir.isDefined) {
        stateConf.MemStore.DataDir.setValue(nodeConf.DataDir().resolve(identifier))
      } else {
        throw new IllegalArgumentException(stateConf.MemStore.DataDir.path + " could not be derived for state: " + identifier)
      }
    }
    if (classOf[AvroRecord].isAssignableFrom(keyClass)) {
      val prefixLen = AvroSerde.binaryPrefixLength(keyClass.asSubclass(classOf[AvroRecord]))
      if (prefixLen.isDefined) stateConf.MemStore.KeyPrefixSize.setValue(prefixLen.get)
    }
    val metrics = AffinityMetrics.forActorSystem(system)
    val kvstore = kvstoreConstructor.newInstance(identifier, stateConf, metrics)
    try {
      create(identifier, partition, stateConf, numPartitions, kvstore, keySerde, valueSerde, metrics)
    } catch {
      case NonFatal(e) => throw new RuntimeException(s"Failed to Configure State $identifier", e)
    }
  }

  def create[K: ClassTag, V: ClassTag](identifier: String,
                                       partition: Int,
                                       stateConf: StateConf,
                                       numPartitions: Int,
                                       kvstore: MemStore,
                                       keySerde: AbstractSerde[K],
                                       valueSerde: AbstractSerde[V],
                                       metrics: AffinityMetrics): KVStoreLocal[K, V] = {
    val ttlMs = if (stateConf.TtlSeconds() < 0) -1L else stateConf.TtlSeconds() * 1000L
    val lockTimeoutMs: lang.Long = stateConf.LockTimeoutMs()
    val minTimestamp = Math.max(stateConf.MinTimestampUnixMs(), if (ttlMs < 0) 0L else EventTime.unix - ttlMs)
    val external = stateConf.External()
    val logOption = if (!stateConf.Storage.isDefined || !stateConf.Storage.Class.isDefined) None else Some {
      val storage = LogStorage.newInstance(stateConf.Storage)
      if (partition == 0) storage.ensureCorrectConfiguration(ttlMs, numPartitions, external)
      if (!external) {
        //if this storage is not managed externally, register key and value subjects in the registry
        for (registry <- asAvroRegistry(keySerde)) {
          Option(storage.keySubject).foreach(some => registry.register(implicitly[ClassTag[K]].runtimeClass, some))
        }
        for (registry <- asAvroRegistry(valueSerde)) {
          Option(storage.valueSubject).foreach(some => registry.register(implicitly[ClassTag[V]].runtimeClass, some))
        }
      }
      storage.reset(partition, TimeRange.since(minTimestamp))
      val checkpointFile = if (!kvstore.isPersistent) null else {
        stateConf.MemStore.DataDir().resolve(kvstore.getClass().getSimpleName() + ".checkpoint")
      }
      storage.open(checkpointFile)
    }
    val keyClass: Class[K] = implicitly[ClassTag[K]].runtimeClass.asInstanceOf[Class[K]]
    new KVStoreLocal(identifier, metrics, kvstore, logOption, partition, keyClass, keySerde, valueSerde, ttlMs, lockTimeoutMs, external)
  }


  private def asAvroRegistry[S](serde: AbstractSerde[S]): Option[AvroSchemaRegistry] = {
    serde match {
      case proxy: AvroSerdeProxy => Some(proxy.internal)
      case registry: AvroSchemaRegistry => Some(registry)
      case _ => None
    }
  }

}


class KVStoreLocal[K, V](val identifier: String,
                         val metrics: AffinityMetrics,
                         kvstore: MemStore,
                         logOption: Option[Log[_]],
                         partition: Int,
                         keyClass: Class[K],
                         keySerde: AbstractSerde[K],
                         valueSerde: AbstractSerde[V],
                         val ttlMs: Long = -1,
                         val lockTimeoutMs: Long = 10000,
                         val external: Boolean = false) extends ObservableState[K] with KVStore[K,V] {

  self =>

  import scala.concurrent.ExecutionContext.Implicits.global

  def optional[T](opt: T): Optional[T] = if (opt != null) Optional.of(opt) else Optional.empty[T]

  def option[T](opt: Optional[T]): Option[T] = if (opt.isPresent) Some(opt.get()) else None

  implicit def javaToScalaFuture[T](jf: java.util.concurrent.Future[T]): Future[T] = Future(jf.get)

  val numKeysMeter = metrics.register(s"state.$identifier.keys", new Gauge[Long] {
    override def getValue = numKeys
  })

  val writesMeter = metrics.meterAndHistogram(s"state.$identifier.writes")

  val readsMeter = metrics.meterAndHistogram(s"state.$identifier.reads")

  def uncheckedMediator(partition: ActorRef, key: Any): Props = {
    Props(new KeyValueMediator(partition, this, key.asInstanceOf[K]))
  }

  private[affinity] def boot(): Unit = logOption.foreach(_
    .bootstrap(identifier, kvstore, partition, optional[ObservableState[K]](if (external) this else null)))

  private[affinity] def tail(): Unit = logOption.foreach(_
    .tail(kvstore, optional[ObservableState[K]](this)))


  /**
    * get an iterator for all records that are strictly not expired
    *
    * @return a weak iterator that doesn't block read and write operations
    */
  def iterator: CloseableIterator[Record[K, V]] = iterator(TimeRange.UNBOUNDED)

  /**
    * get iterator for all records that are within a given time range and an optional prefix sequence
    *
    * @param range  time range to filter the records by
    * @param prefix vararg sequence for the compound key to match; can be empty
    * @return a weak iterator that doesn't block read and write operations
    */
  def iterator(range: TimeRange, prefix: Any*): CloseableIterator[Record[K, V]] = new CloseableIterator[Record[K, V]] {
    val javaPrefix = prefix.map(_.asInstanceOf[AnyRef])
    val bytePrefix: ByteBuffer = if (javaPrefix.isEmpty) null else {
      ByteBuffer.wrap(keySerde.prefix(keyClass, javaPrefix: _*))
    }
    val underlying = kvstore.iterator(bytePrefix)
    val mapped = underlying.asScala.flatMap { entry =>
      option(kvstore.unwrap(entry.getKey(), entry.getValue, ttlMs))
        .filter(byteRecord => range.contains(byteRecord.timestamp))
        .map { byteRecord =>
          val key = keySerde.fromBytes(byteRecord.key)
          val value = valueSerde.fromBytes(byteRecord.value)
          new Record(key, value, byteRecord.timestamp)
        }
    }

    override def next(): Record[K, V] = mapped.next()

    override def hasNext: Boolean = mapped.hasNext

    override def close(): Unit = underlying.close()
  }

  /**
    * Retrieve a value from the store asynchronously
    *
    * @param key to retrieve value of
    * @return Future.Success(Some(V)) if the key exists and the value could be retrieved and deserialized
    *         Future.Success(None) if the key doesn't exist
    *         Future.Failed(Throwable) if a non-fatal exception occurs
    */
  def apply(key: K): Option[V] = apply(ByteBuffer.wrap(keySerde.toBytes(key)))

  private def apply(key: ByteBuffer): Option[V] = {
    val timerContext = readsMeter.markStart()
    try {
      for (
        cell: ByteBuffer <- option(kvstore(key));
        byteRecord: Record[Array[Byte], Array[Byte]] <- option(kvstore.unwrap(key, cell, ttlMs))
      ) yield {
        val result = valueSerde.fromBytes(byteRecord.value)
        readsMeter.markSuccess(timerContext)
        result
      }
    } catch {
      case e: Throwable =>
        readsMeter.markFailure(timerContext)
        throw e
    }
  }

  /**
    * Get all records that match the given time range and optional prefix sequence
    *
    * @param range   time range to filter the records by
    * @param prefix1 mandatory root prefix
    * @param prefixN optional secondary prefix sequence
    * @return Map[K,V] as a transformation of Record.key -> Record.value
    */
  def range(range: TimeRange, prefix1: Any, prefixN: Any*): Map[K, V] = {
    val timerContext = readsMeter.markStart()
    try {
      val builder = Map.newBuilder[K, V]
      val it = iterator(range, (prefix1 +: prefixN): _*)
      try {
        it.asScala.foreach(record => builder += record.key -> record.value)
        val result = builder.result()
        readsMeter.markSuccess(timerContext, result.size.toLong)
        result
      } finally {
        it.close()
      }
    } catch {
      case e: Throwable =>
        readsMeter.markFailure(timerContext)
        throw e
    }
  }

  /**
    * @return numKeys hint - this may or may not be accurate, depending on the underlying backend's features
    */
  def numKeys: Long = kvstore.numKeys()

  /**
    * replace is a faster operation than update because it doesn't look at the existing value
    * associated with the given key
    *
    * @param key   to update
    * @param value new value to be associated with the key
    * @return Future which if successful holds either:
    *         Success(Some(value)) if the write was persisted
    *         Success(None) if the write was persisted but the operation resulted in the value was expired immediately
    *         Failure(ex) if the operation failed due to exception
    */
  def replace(key: K, value: V): Future[Option[V]] = {
    val l = lock(key)
    try {
      put(keySerde.toBytes(key), value).transform(w => {
        unlock(key, l)
        push(key, w)
        w
      }, f => {
        unlock(key, l)
        f
      })
    } catch {
      case e: Throwable =>
        unlock(key, l)
        throw e
    }
  }

  /**
    * delete the given key
    *
    * @param key to delete
    * @return Future which may be either:
    *         Success(None) if the key was deleted
    *         Failure(ex) if the operation failed due to exception
    */
  def delete(key: K): Future[Option[V]] = {
    val l = lock(key)
    try {
      delete(keySerde.toBytes(key)).transform(w => {
        unlock(key, l)
        push(key, w);
        w
      }, f => {
        unlock(key, l)
        f
      })
    } catch {
    case e: Throwable =>
      unlock(key, l)
      throw e
    }
  }

  /**
    * insert is a syntactic sugar for update which is only executed if the key doesn't exist yet
    *
    * @param key   to insert
    * @param value new value to be associated with the key
    * @return Future value newly inserted if the key did not exist and operation succeeded, failed future otherwise
    */
  def insert(key: K, value: V): Future[V] = updateAndGet(key, x => x match {
    case Some(_) => throw new IllegalArgumentException(s"$key already exists in state store")
    case None => Some(value)
  }).map(_.get)

  /**
    * atomic get-and-update if the current and updated value are the same the no modifications are made to
    * the underlying stores and the returned future is completed immediately.
    *
    * @param key to updateImpl
    * @param f   function which given a current value returns an updated value or empty if the key is to be removed
    *            as a result of the update
    * @return Future Optional of the value previously held at the key position
    */
  def getAndUpdate(key: K, f: Option[V] => Option[V]): Future[Option[V]] = try {
    val k = keySerde.toBytes(key)
    val l = lock(key)
    try {
      val currentValue = apply(ByteBuffer.wrap(k))
      val updatedValue = f(currentValue)
      if (currentValue == updatedValue) {
        unlock(key, l)
        Future.successful(currentValue)
      } else {
        val f = if (updatedValue.isDefined) put(k, updatedValue.get) else delete(k)
        f.transform({
          s => unlock(key, l); s
        }, {
          e => unlock(key, l); e
        }) andThen {
          case _ => push(key, updatedValue)
        } map (_ => currentValue)
      }
    } catch {
      case e: Throwable =>
        unlock(key, l)
        throw e
    }
  } catch {
    case NonFatal(e) => Future.failed(e)
  }

  /**
    * atomic update-and-get - if the current and updated value are the same the no modifications are made to
    * the underlying stores and the returned future is completed immediately.
    *
    * @param key key which is going to be updated
    * @param f   function which given a current value returns an updated value or empty if the key is to be removed
    *            as a result of the update
    * @return Future optional value which will be successful if the put operation succeeded and will hold the updated value
    */
  def updateAndGet(key: K, f: Option[V] => Option[V]): Future[Option[V]] = {
    try {
      val k = keySerde.toBytes(key)
      val l = lock(key)
      try {
        val currentValue = apply(ByteBuffer.wrap(k))
        val updatedValue = f(currentValue)
        if (currentValue == updatedValue) {
          unlock(key, l)
          Future.successful(updatedValue)
        } else {
          val f = if (updatedValue.isDefined) put(k, updatedValue.get) else delete(k)
          f.transform({
            s => unlock(key, l); s
          }, {
            e => unlock(key, l); e
          }) andThen {
            case _ => push(key, updatedValue)
          } map (_ => updatedValue)
        }
      } catch {
        case e: Throwable =>
          unlock(key, l)
          throw e
      }
    } catch {
      case NonFatal(e) => Future.failed(e)
    }
  }

  /**
    * An asynchronous non-blocking put operation which inserts or updates the value
    * at the given key. The value is first updated in the kvstore and then a future is created
    * for reflecting the modification in the underlying storage. If the storage write fails
    * the previous value is rolled back in the kvstore and the failure is propagated into
    * the result future.
    *
    * @param key   serialized key
    * @param value new value for the key
    * @return future of the checkpoint that will represent the consistency information after the operation completes
    */
  private def put(key: Array[Byte], value: V): Future[Option[V]] = {
    if (external) throw new IllegalStateException("put() called on a read-only state")
    val nowMs = EventTime.unix
    val recordTimestamp = value match {
      case e: EventTime => e.eventTimeUnix()
      case _ => nowMs
    }
    if (ttlMs > 0 && recordTimestamp + ttlMs < nowMs) {
      delete(key)
    } else {
      val timerContext = writesMeter.markStart()
      try {
        val valueBytes = valueSerde.toBytes(value)
        logOption match {
          case None =>
            kvstore.put(ByteBuffer.wrap(key), kvstore.wrap(valueBytes, recordTimestamp))
            writesMeter.markSuccess(timerContext)
            Future.successful(Some(value))
          case Some(log) =>
            log.append(kvstore, key, valueBytes, recordTimestamp) transform(
              pos => { //TODO pos is currently not used but could be instrumental in direct synchronization of standby replicas
                writesMeter.markSuccess(timerContext)
                Some(value)
              },
              error => {
                writesMeter.markFailure(timerContext)
                error
              }
            )
        }
      } catch {
        case e: Throwable =>
          writesMeter.markFailure(timerContext)
          throw e
      }
    }
  }

  /**
    * An asynchronous non-blocking removal operation which deletes the value
    * at the given key. The value is first removed from the kvstore and then a future is created
    * for reflecting the modification in the underlying storage. If the storage write fails
    * the previous value is rolled back in the kvstore and the failure is propagated into
    * the result future.
    *
    * @param key serialized key to delete
    * @return future of the checkpoint that will represent the consistency information after the operation completes
    */
  private def delete(key: Array[Byte]): Future[Option[V]] = {
    if (external) throw new IllegalStateException("delete() called on a read-only state")
    val timerContext = writesMeter.markStart()
    try {
      logOption match {
        case None =>
          kvstore.remove(ByteBuffer.wrap(key))
          writesMeter.markSuccess(timerContext)
          Future.successful(None)
        case Some(log) =>
          log.delete(kvstore, key) transform(
            _ => {
              writesMeter.markSuccess(timerContext)
              None
            }, error => {
            writesMeter.markFailure(timerContext)
            error
          })
      }
    } catch {
      case e: Throwable =>
        writesMeter.markFailure(timerContext)
        throw e
    }

  }

  /*
   * Observable State Support
   */

  /**
    * State listeners can be instantiated at the partition level and are notified for any change in this State.
    */
  def listen(pf: PartialFunction[(K, Any), Unit]): Unit = {
    addObserver(new Observer {
      override def update(o: Observable, arg: scala.Any) = arg match {
        case entry: java.util.Map.Entry[K, _] => pf.lift((entry.getKey, entry.getValue))
        case (key: Any, event: Any) => pf.lift((key.asInstanceOf[K], event))
        case illegal => throw new RuntimeException(s"Can't send $illegal to observers")
      }
    })
  }

  override def internalPush(key: Array[Byte], value: Optional[Array[Byte]]) = {
    if (value.isPresent) {
      push(keySerde.fromBytes(key), Some(valueSerde.fromBytes(value.get)))
    } else {
      push(keySerde.fromBytes(key), None)
    }
  }

  override def close() = {
    try {
      logOption.foreach(_.close())
    } finally {
      kvstore.close()
      metrics.remove(s"state.$identifier.keys")
    }
  }

  /**
    * row locking functionality
    */
  private class RowLock extends AnyRef

  private val locks = new ConcurrentHashMap[K, RowLock]

  private def lock(key: K): RowLock = {
    val lock = new RowLock()
    breakable {
      val start = System.currentTimeMillis
      do {
        val existingLock = locks.putIfAbsent(key, lock)
        if (existingLock == null) break else existingLock.synchronized {
          existingLock.wait(lockTimeoutMs)
          val waitedMs = System.currentTimeMillis - start
          if (waitedMs >= lockTimeoutMs) {
            throw new TimeoutException(s"Could not acquire lock for $key in $lockTimeoutMs ms")
          }
        }
      } while (true)
    }
    return lock
  }

  private def unlock(key: K, lock: RowLock): Unit = {
    if (!locks.remove(key, lock)) {
      throw new IllegalMonitorStateException(s"$key is locked by another operation")
    } else lock.synchronized {
      lock.notifyAll()
    }
  }

}
