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
import io.amient.affinity.Conf
import io.amient.affinity.avro.AvroSchemaRegistry
import io.amient.affinity.avro.record.{AvroRecord, AvroSerde}
import io.amient.affinity.core.actor.KeyValueMediator
import io.amient.affinity.core.serde.avro.AvroSerdeProxy
import io.amient.affinity.core.serde.{AbstractSerde, Serde}
import io.amient.affinity.core.state.KVStoreLocal.configureMemStoreDataDir
import io.amient.affinity.core.storage._
import io.amient.affinity.core.util._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.language.{existentials, implicitConversions}
import scala.reflect.ClassTag
import scala.util.control.Breaks._
import scala.util.control.NonFatal

object KVStoreLocal {


  def configureMemStoreDataDir(identifier: String, stateConf: StateConf, system: ActorSystem, metrics: AffinityMetrics) = {
    if (!stateConf.MemStore.DataDir.isDefined) {
      val nodeConf = Conf(system.settings.config).Affi.Node
      if (nodeConf.DataDir.isDefined) {
        stateConf.MemStore.DataDir.setValue(nodeConf.DataDir().resolve(identifier))
      } else {
        stateConf.MemStore.DataDir.setValue(null)
      }
    }
  }

  def create[K: ClassTag, V: ClassTag](name: String,
                                       partition: Int,
                                       stateConf: StateConf,
                                       numPartitions: Int,
                                       system: ActorSystem): KVStoreLocal[K, V] = {
    val identifier = if (partition < 0) name else s"$name-$partition"
    val keySerde = Serde.of[K](system.settings.config)
    val valueSerde = Serde.of[V](system.settings.config)
    val keyClass = implicitly[ClassTag[K]].runtimeClass
    if (classOf[AvroRecord].isAssignableFrom(keyClass)) {
      val prefixLen = AvroSerde.binaryPrefixLength(keyClass.asSubclass(classOf[AvroRecord]))
      if (prefixLen.isDefined) stateConf.MemStore.KeyPrefixSize.setValue(prefixLen.get)
    }
    val metrics = AffinityMetrics.forActorSystem(system)
    configureMemStoreDataDir(identifier, stateConf, system, metrics)
    val memstoreClass = stateConf.MemStore.Class()
    val memstoreConstructor = memstoreClass.getConstructor(classOf[String], classOf[StateConf], classOf[MetricRegistry])
    val memstore = memstoreConstructor.newInstance(identifier, stateConf, metrics)
    try {
      create(identifier, partition, stateConf, numPartitions, memstore, keySerde, valueSerde, system)
    } catch {
      case NonFatal(e) => throw new RuntimeException(s"Failed to Configure State $identifier", e)
    }
  }

  def create[K: ClassTag, V: ClassTag](identifier: String,
                                       partition: Int,
                                       stateConf: StateConf,
                                       numPartitions: Int,
                                       memstore: MemStore,
                                       keySerde: AbstractSerde[K],
                                       valueSerde: AbstractSerde[V],
                                       system: ActorSystem): KVStoreLocal[K, V] = {
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
      val checkpointFile = if (!memstore.isPersistent) null else {
        stateConf.MemStore.DataDir().resolve(memstore.getClass().getSimpleName() + ".checkpoint")
      }
      storage.open(checkpointFile)
    }
    val keyClass: Class[K] = implicitly[ClassTag[K]].runtimeClass.asInstanceOf[Class[K]]
    new KVStoreLocal(
      identifier,
      stateConf,
      memstore,
      logOption,
      partition,
      keyClass,
      keySerde,
      valueSerde,
      ttlMs,
      lockTimeoutMs,
      external,
      system
    )
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
                         stateConf: StateConf,
                         memstore: MemStore,
                         logOption: Option[Log[_]],
                         partition: Int,
                         keyClass: Class[K],
                         keySerde: AbstractSerde[K],
                         valueSerde: AbstractSerde[V],
                         val ttlMs: Long = -1,
                         val lockTimeoutMs: Long = 10000,
                         val external: Boolean = false,
                         system: ActorSystem
                        ) extends ObservableState[K] with KVStore[K, V] {

  self =>

  import scala.concurrent.ExecutionContext.Implicits.global

  val metrics = AffinityMetrics.forActorSystem(system)

  private val logger = LoggerFactory.getLogger(this.getClass)

  def optional[T](opt: T): Optional[T] = if (opt != null) Optional.of(opt) else Optional.empty[T]

  def option[T](opt: Optional[T]): Option[T] = if (opt.isPresent) Some(opt.get()) else None

  implicit def javaToScalaFuture[T](jf: java.util.concurrent.Future[T]): Future[T] = Future(jf.get)

  try {
    metrics.register(s"state.$identifier.keys", new Gauge[Long] {
      override def getValue = numKeys
    })
  } catch {
    case e: IllegalArgumentException =>
      //TODO this only stops creating of the same metric on an individual host, but we need the same behaviour also across the cluster
      //becuase each replica will register the same set of metrics which, though can be mitigated when aggregating metrics, may lead to confusion and is a pointless overhead
      logger.warn(e.getMessage)
  }

  val writesMeter = metrics.meterAndHistogram(s"state.$identifier.writes")

  val readsMeter = metrics.meterAndHistogram(s"state.$identifier.reads")

  def uncheckedMediator(partition: ActorRef, key: Any): Props = {
    Props(new KeyValueMediator(partition, this, key.asInstanceOf[K]))
  }

  private val indicies = scala.collection.mutable.ArrayBuffer[KVStoreIndex[_, _]]()

  def index[IK: ClassTag](indexName: String)(indexFunction: Record[K, V] => List[IK]): KVStoreIndex[IK, K] = {
    val indexIdentifier = s"$identifier-$indexName"
    logger.info(s"Opening index: $indexIdentifier")
    val indexKeySerde = Serde.of[IK](system.settings.config)
    val indexConf = new StateConf().apply(stateConf)
    logOption match {
      case None => indexConf.MemStore.Class.setValue(classOf[MemStoreSimpleMap])
      case Some(log) => indexConf.External.setValue(true)
    }
    configureMemStoreDataDir(indexIdentifier, indexConf, system, metrics)
    indexConf.MemStore.KeyPrefixSize.setValue(4)
    val mustRebuild = !indexConf.MemStore.DataDir.isDefined || !indexConf.MemStore.DataDir().resolve("initialized").toFile.exists()
    val memstoreClass = indexConf.MemStore.Class()
    val memstoreConstructor = memstoreClass.getConstructor(classOf[String], classOf[StateConf], classOf[MetricRegistry])
    val indexMemStore = memstoreConstructor.newInstance(indexIdentifier, indexConf, metrics)
    val indexStore = new KVStoreIndex[IK, K](indexIdentifier, indexMemStore, indexKeySerde, keySerde, ttlMs)
    indicies += indexStore

    def doIndexRecord(record: Record[K, V]): Unit = {
      //TODO #242 / #248 rocksdb writing asynchronously indicies needs to participate in the pipeline guarantees
      indexFunction(record).distinct.map {
        case ik if record.tombstone => indexStore.put(ik, record.key, record.timestamp, tombstone = true)
        case ik => indexStore.put(ik, record.key, record.timestamp)
      }
    }

    if (mustRebuild) {
      //rebuilding index is done not from the underlying long but from the state store iterator which is being indexed
      //in terms of consistency it seems it would be better to have it initialized from the underyling log
      //and simply continue tailing but there are several reasons why to use state store iterator
      //1. it performs better (assuming truly local storage)
      //2. in principle indicies should live as close to the data they indexing as possible
      //3. deindexing wouldn't work because the previous value is not known for tombstones at the log-level
      val allIterator = iterator
      try {
        logger.info(s"Rebuilding index: $indexIdentifier ...")
        memstore.erase()
        allIterator.asScala.foreach(doIndexRecord)
        if (indexConf.MemStore.DataDir.isDefined) {
          indexConf.MemStore.DataDir().resolve("initialized").toFile.createNewFile()
        }
      } finally {
        allIterator.close()
      }
    }

    //start tailing the kvstore for changes that need to be applied on the index
    this.listen {
      //TODO #242 / #248 what kind of guarantees does this have
      case record: Record[K, V] => doIndexRecord(record)
    }

    indexStore
  }


  private[affinity] def boot(): Unit = logOption.foreach(_
    .bootstrap(identifier, memstore, partition, optional[ObservableState[K]](if (external) this else null)))

  private[affinity] def tail(): Unit = logOption.foreach(_
    .tail(memstore, optional[ObservableState[K]](this)))


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
    val underlying = memstore.iterator(bytePrefix)
    val mapped = underlying.asScala.flatMap { entry =>
      option(memstore.unwrap(entry.getKey(), entry.getValue, ttlMs))
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
        cell: ByteBuffer <- option(memstore(key));
        byteRecord: Record[Array[Byte], Array[Byte]] <- option(memstore.unwrap(key, cell, ttlMs))
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
  def numKeys: Long = memstore.numKeys()

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
    lockAsync(key) {
      put(keySerde.toBytes(key), value).map { w =>
        push(new Record(key, value))
        w
      }
    }
  }

  def insert(key: K, value: V): Future[V] = updateAndGet(key, x => x match {
    case Some(_) => throw new IllegalArgumentException(s"$key already exists in state store")
    case None => Some(value)
  }).map(_.get)

  /**
    * remove the key
    *
    * @param key to delete
    * @return Future which may be either:
    *         Success(Some(removedValue)) if the key existed and was deleted
    *         Success(None) if the key didn't exist
    *         Failure(ex) if the operation failed due to exception
    */
  def delete(key: K): Future[Option[V]] = {
    if (external) throw new IllegalStateException("delete() called on a read-only state")
    lockAsync(key) {
      val keyBytes = keySerde.toBytes(key)
      apply(ByteBuffer.wrap(keyBytes)) match {
        case None => Future.successful(None)
        case Some(removedValue) =>
          delete(keyBytes)
            .map ( _ =>  push(new Record(key, removedValue, EventTime.unix, true)))
            .map ( _ => Some(removedValue))
      }
    }
  }

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
    lockAsync(key) {
      val currentValue: Option[V] = apply(ByteBuffer.wrap(k))
      val updatedValue: Option[V] = f(currentValue)
      if (currentValue == updatedValue) {
        Future.successful(currentValue)
      } else {
        val f = if (updatedValue.isDefined) put(k, updatedValue.get) else delete(k)
        f.andThen {
          case _ => updatedValue match {
            case Some(value) => push(new Record[K, V](key, value))
            case None => currentValue.foreach(c => push(new Record[K, V](key, c, true)))
          }
        } map (_ => currentValue)
      }
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
      lockAsync(key) {
        val currentValue = apply(ByteBuffer.wrap(k))
        val updatedValue: Option[V] = f(currentValue)
        if (currentValue == updatedValue) {
          Future.successful(updatedValue)
        } else {
          val f = if (updatedValue.isDefined) put(k, updatedValue.get) else delete(k)
          f andThen {
            case _ => updatedValue match {
              case Some(value) => push(new Record[K, V](key, value))
              case None => currentValue.foreach(c => push(new Record[K, V](key, c, true)))
            }
          } map (_ => updatedValue)
        }
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
      delete(key) map (_ => None)
    } else {
      val timerContext = writesMeter.markStart()
      try {
        val valueBytes = valueSerde.toBytes(value)
        logOption match {
          case None =>
            memstore.put(ByteBuffer.wrap(key), memstore.wrap(valueBytes, recordTimestamp))
            writesMeter.markSuccess(timerContext)
            Future.successful(Some(value))
          case Some(log) =>
            log.append(memstore, key, valueBytes, recordTimestamp) transform(
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
    * @return future of unt which completes when the delete was persisted in both memstore and the log
    */
  private def delete(key: Array[Byte]): Future[Unit] = {
    if (external) throw new IllegalStateException("delete() called on a read-only state")
    val timerContext = writesMeter.markStart()
    try {
      logOption match {
        case None =>
          memstore.remove(ByteBuffer.wrap(key))
          writesMeter.markSuccess(timerContext)
          Future.successful(None)
        case Some(log) =>
          log.delete(memstore, key) transform(
            _ => {
              writesMeter.markSuccess(timerContext)
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
  def listen(pf: PartialFunction[Record[K, V], Unit]): Unit = {
    addObserver(new Observer {
      override def update(o: Observable, arg: scala.Any) = arg match {
        case entry: Record[K, V] => pf.lift(entry)
        case illegal => throw new RuntimeException(s"Can't send $illegal to observers")
      }
    })
  }

  override def internalPush(record: Record[Array[Byte], Array[Byte]]) = {
    push(new Record(
      keySerde.fromBytes(record.key),
      valueSerde.fromBytes(record.value),
      record.timestamp,
      record.tombstone))
  }

  override def close() = {
    try {
      logOption.foreach(_.close())
    } finally {
      memstore.close()
      metrics.remove(s"state.$identifier.keys")
      indicies.foreach(_.close)
    }
  }

  /**
    * row locking functionality
    */
  private class RowLock extends AnyRef

  private val locks = new ConcurrentHashMap[Any, RowLock]

  def lockAsync[T](scope: Any)(body: => Future[T]): Future[T] = {
    lock(scope)
    try {
      body.transform(
        (s) => {
          unlock(scope)
          s
        },
        (f) => {
          unlock(scope)
          f
        }
      )
    } catch {
      case NonFatal(e) =>
        unlock(scope)
        throw e
    }
  }

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

  /**
    * @return statistics about the memstore and storage, whatever is available
    */
  override def getStats: String = {
    s"$identifier\n===================================================================\n" +
      s"${logOption.map(_.getStats).getOrElse("")}\nMemStore[${memstore.getClass.getSimpleName}]\n${memstore.getStats}\n\n"
  }
}
