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

package io.amient.affinity.core.storage

import java.io.Closeable
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, TimeoutException}
import java.util.{Observable, Observer, Optional}

import akka.actor.{ActorRef, ActorSystem, Props}
import com.typesafe.config.Config
import io.amient.affinity.Conf
import io.amient.affinity.avro.AvroSchemaRegistry
import io.amient.affinity.core.actor.KeyValueMediator
import io.amient.affinity.core.serde.avro.AvroSerdeProxy
import io.amient.affinity.core.serde.{AbstractSerde, Serde}
import io.amient.affinity.core.util.{CloseableIterator, EventTime, TimeRange}

import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.language.{existentials, postfixOps}
import scala.reflect.ClassTag
import scala.util.control.NonFatal

object State {

  object StateConf extends StateConf {
    override def apply(config: Config): StateConf = new StateConf().apply(config)
  }

  def create[K: ClassTag, V: ClassTag](identifier: String, partition: Int, stateConf: StateConf, numPartitions: Int, system: ActorSystem): State[K, V] = {
    val keySerde = Serde.of[K](system.settings.config)
    val valueSerde = Serde.of[V](system.settings.config)
    val kvstoreClass = stateConf.MemStore.Class()
    val kvstoreConstructor = kvstoreClass.getConstructor(classOf[StateConf])
    if (!stateConf.MemStore.DataDir.isDefined) {
      val nodeConf = Conf(system.settings.config).Affi.Node
      if (nodeConf.DataDir.isDefined) {
        stateConf.MemStore.DataDir.setValue(nodeConf.DataDir().resolve(identifier))
      } else {
        throw new IllegalArgumentException(stateConf.MemStore.DataDir.path + " could not be derived for state: " + identifier)
      }
    }
    val kvstore = kvstoreConstructor.newInstance(stateConf)
    try {
      create(partition, stateConf, numPartitions, kvstore, keySerde, valueSerde)
    } catch {
      case NonFatal(e) => throw new RuntimeException(s"Failed to Configure State $identifier", e)
    }
  }

  def create[K: ClassTag, V: ClassTag](partition: Int, stateConf: StateConf, numPartitions: Int, kvstore: MemStore, keySerde: AbstractSerde[K], valueSerde: AbstractSerde[V]): State[K, V] = {
    val ttlMs = if (stateConf.TtlSeconds() < 0) -1L else stateConf.TtlSeconds() * 1000L
    val lockTimeoutMs = stateConf.LockTimeoutMs()
    val minTimestamp = Math.max(stateConf.MinTimestampUnixMs(), if (ttlMs < 0) 0L else EventTime.unix - ttlMs)
    val external = stateConf.External()
    val logOption = if (!stateConf.Storage.isDefined) None else Some {
      val storage = LogStorage.newInstance(stateConf.Storage)
      storage.ensureCorrectConfiguration(ttlMs, numPartitions, external)
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
    new State(kvstore, logOption, partition, keySerde, valueSerde, ttlMs, lockTimeoutMs, readonly = external)
  }


  private def asAvroRegistry[S](serde: AbstractSerde[S]): Option[AvroSchemaRegistry] = {
    serde match {
      case proxy: AvroSerdeProxy => Some(proxy.internal)
      case registry: AvroSchemaRegistry => Some(registry)
      case _ => None
    }
  }

}


class State[K, V](kvstore: MemStore,
                  logOption: Option[Log[_]],
                  partition: Int,
                  keySerde: AbstractSerde[K],
                  valueSerde: AbstractSerde[V],
                  val ttlMs: Long = -1,
                  val lockTimeoutMs: Int = 10000,
                  val readonly: Boolean = false) extends ObservableState[K] with Closeable {

  self =>

  import scala.concurrent.ExecutionContext.Implicits.global

  def option[T](opt: Optional[T]): Option[T] = if (opt.isPresent) Some(opt.get()) else None

  implicit def javaToScalaFuture[T](jf: java.util.concurrent.Future[T]): Future[T] = Future(jf.get)

  def uncheckedMediator(partition: ActorRef, key: Any): Props = {
    Props(new KeyValueMediator(partition, this, key.asInstanceOf[K]))
  }

  private[affinity] def boot(): Unit = logOption.foreach(_.bootstrap(kvstore, partition))

  private[affinity] def tail(): Unit = logOption.foreach(_.tail(kvstore, this))

  /**
    * @return a weak iterator that doesn't block read and write operations
    */
  def iterator: CloseableIterator[Record[K, V]] = new CloseableIterator[Record[K, V]] {
    val underlying = kvstore.iterator
    val mapped = underlying.flatMap { entry =>
      val key = keySerde.fromBytes(entry.getKey.array())
      option(kvstore.unwrap(entry.getKey(), entry.getValue, ttlMs)).map {
        byteRecord => new Record(key, valueSerde.fromBytes(byteRecord.value), byteRecord.timestamp)
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
    for (
      cell: ByteBuffer <- option(kvstore(key));
      byteRecord: Record[Array[Byte], Array[Byte]] <- option(kvstore.unwrap(key, cell, ttlMs))
    ) yield valueSerde.fromBytes(byteRecord.value)
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
    * @return Unit Future which may be failed if the operation didn't succeed
    */
  def replace(key: K, value: V): Future[Unit] = put(keySerde.toBytes(key), value).map(_ => push(key, value))

  /**
    * delete the given key
    *
    * @param key to delete
    * @return Unit Future which may be failed if the operation didn't succeed
    */
  def delete(key: K): Future[Unit] = delete(keySerde.toBytes(key)).map(_ => push(key, null))

  /**
    * update is a syntactic sugar for update where the value is always overriden unless the current value is the same
    *
    * @param key   to updateImpl
    * @param value new value to be associated with the key
    * @return Future Optional of the value previously held at the key position
    */
  def update(key: K, value: V): Future[Option[V]] = getAndUpdate(key, _ => Some(value))

  /**
    * remove is a is a syntactic sugar for update where None is used as Value
    * it is different from delete in that it returns the removed value
    * which is more costly.
    *
    * @param key     to remove
    * @param command is the message that will be pushed to key-value observers
    * @return Future Optional of the value previously held at the key position
    */
  def remove(key: K, command: Any): Future[Option[V]] = getAndUpdate(key, x => None)

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
    * This is similar to apply(key) except it also applies row lock which is useful if the client
    * wants to make sure that the returned value incorporates all changes applied to it in update operations
    * within the same sequence.
    *
    * @param key
    * @return
    */
  def get(key: K): Option[V] = {
    val l = lock(key)
    try {
      apply(key)
    } finally {
      unlock(key, l)
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
    * @param f function which given a current value returns an updated value or empty if the key is to be removed
    *          as a result of the update
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
    * update enables per-key observer pattern for incremental updates
    *
    * @param key  key which is going to be updated
    * @param pf   putImpl function which maps the current value Option[V] at the given key to 3 values:
    *             1. Option[Any] is the incremental putImpl event
    *             2. Option[V] is the new state for the given key as a result of the incremntal putImpl
    *             3. R which is the result value expected by the caller
    * @return Future[R] which will be successful if the put operation of Option[V] of the pf succeeds
    */
  def updatedAndMap[R](key: K)(pf: PartialFunction[Option[V], (Option[Any], Option[V], R)]): Future[R] = {
    try {
      val k = keySerde.toBytes(key)
      val l = lock(key)
      try {
        pf(apply(ByteBuffer.wrap(k))) match {
          case (None, _, result) =>
            unlock(key, l)
            Future.successful(result)
          case (Some(increment), changed, result) => changed match {
            case Some(updatedValue) =>
              put(k, updatedValue) transform( {
                s => unlock(key, l); s
              }, {
                e => unlock(key, l); e
              }) andThen {
                case _ => push(key, increment)
              } map (_ => result)
            case None =>
              delete(k) transform( {
                s => unlock(key, l); s
              }, {
                e => unlock(key, l); e
              }) andThen {
                case _ => push(key, increment)
              } map (_ => result)
          }
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
  private def put(key: Array[Byte], value: V): Future[_] = {
    if (readonly) throw new IllegalStateException("put() called on a read-only state")
    val nowMs = System.currentTimeMillis()
    val recordTimestamp = value match {
      case e: EventTime => e.eventTimeUnix()
      case _ => nowMs
    }
    if (ttlMs > 0 && recordTimestamp + ttlMs < nowMs) {
      delete(key)
    } else {
      val valueBytes = valueSerde.toBytes(value)
      logOption match {
        case None => Future.successful(kvstore.put(ByteBuffer.wrap(key), kvstore.wrap(valueBytes, recordTimestamp)))
        case Some(log) => log.append(kvstore, key, valueBytes, recordTimestamp)
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
  private def delete(key: Array[Byte]): Future[_] = {
    if (readonly) throw new IllegalStateException("delete() called on a read-only state")
    logOption match {
      case None => Future.successful(kvstore.remove(ByteBuffer.wrap(key)))
      case Some(log) => log.delete(kvstore, key)
    }
  }

  /*
   * Observable State Support
   */

  /**
    * State listeners can be instantiated at the partition level and are notified for any change in this State.
    */
  //FIXMe this has to become listen(state)(pf) of the ActorState
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
    }
  }

  /**
    * row locking functionality
    */

  private val locks = new ConcurrentHashMap[K, java.lang.Long]

  private def unlock(key: K, l: java.lang.Long): Unit = {
    if (!locks.remove(key, l)) {
      throw new IllegalMonitorStateException(s"$key is locked by another Thread")
    }
  }

  private def lock(key: K): java.lang.Long = {
    val l = Thread.currentThread.getId
    var counter = 0
    val start = System.currentTimeMillis
    while (locks.putIfAbsent(key, l) != null) {
      counter += 1
      val sleepTime = math.log(counter).round
      if (sleepTime > 0) {
        if (System.currentTimeMillis - start > lockTimeoutMs) {
          throw new TimeoutException(s"Could not acquire lock for $key in $lockTimeoutMs ms")
        } else {
          Thread.sleep(sleepTime)
        }
      }
    }
    l
  }

}
