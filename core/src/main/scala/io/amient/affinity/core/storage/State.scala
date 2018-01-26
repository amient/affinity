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

import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, TimeoutException}
import java.util.{Observable, Observer, Optional}

import akka.actor.{ActorRef, ActorSystem, Props}
import com.typesafe.config.Config
import io.amient.affinity.core.actor.KeyValueMediator
import io.amient.affinity.core.cluster.Node
import io.amient.affinity.core.serde.{AbstractSerde, Serde}
import io.amient.affinity.core.util.{ByteUtils, EventTime}
import io.amient.affinity.stream.Record

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.language.{existentials, postfixOps}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.control.NonFatal

object State {

  object StateConf extends StateConf {
    override def apply(config: Config): StateConf = new StateConf().apply(config)
  }

  def create[K: ClassTag, V: ClassTag](identifier: String, partition: Int, stateConf: StateConf, numPartitions: Int, system: ActorSystem): State[K, V] = {
    val ttlMs = if (stateConf.TtlSeconds() < 0) -1L else stateConf.TtlSeconds() * 1000L
    val lockTimeoutMs = stateConf.LockTimeoutMs()
    val readonly = stateConf.External()
    val storage = try if (!stateConf.Storage.isDefined) new NoopStorage(identifier, stateConf, partition, 1) else {
      if (!stateConf.MemStore.DataDir.isDefined) {
        val conf = Node.Conf(system.settings.config)
        if (conf.Affi.DataDir.isDefined) stateConf.MemStore.DataDir.setValue(conf.Affi.DataDir())
      }
      val storageClass = stateConf.Storage.Class()
      val storageClassSymbol = rootMirror.classSymbol(storageClass)
      val storageClassMirror = rootMirror.reflectClass(storageClassSymbol)
      val constructor = storageClassSymbol.asClass.primaryConstructor.asMethod
      val constructorMirror = storageClassMirror.reflectConstructor(constructor)
      constructorMirror(identifier, stateConf, partition, numPartitions).asInstanceOf[Storage]
    } catch {
      case NonFatal(e) => throw new RuntimeException(s"Failed to Configure State $identifier", e)
    }
    val keySerde = Serde.of[K](system.settings.config)
    val valueSerde = Serde.of[V](system.settings.config)
    new State[K, V](storage, keySerde, valueSerde, ttlMs, lockTimeoutMs, readonly)
  }

}


class State[K, V](val storage: Storage,
                  keySerde: AbstractSerde[K],
                  valueSerde: AbstractSerde[V],
                  val ttlMs: Long = -1,
                  val lockTimeoutMs: Int = 10000,
                  val external: Boolean = false) extends ObservableState[K] {

  self =>

  import scala.concurrent.ExecutionContext.Implicits.global

  def option[T](opt: Optional[T]): Option[T] = if (opt.isPresent) Some(opt.get()) else None

  implicit def javaToScalaFuture[T](jf: java.util.concurrent.Future[T]): Future[T] = Future(jf.get)

  def uncheckedMediator(partition: ActorRef, key: Any): Props = {
    Props(new KeyValueMediator(partition, this, key.asInstanceOf[K]))
  }

  /**
    * @return a weak iterator that doesn't block read and write operations
    */
  def iterator: CloseableIterator[(K, V)] = new CloseableIterator[(K, V)] {
    val underlying = storage.memstore.iterator
    val mapped = underlying.asScala.flatMap { entry =>
      val key = keySerde.fromBytes(entry.getKey.array())
      option(storage.memstore.unwrap(entry.getKey(), entry.getValue, ttlMs)).map {
        bytes => (key, valueSerde.fromBytes(bytes))
      }
    }

    override def next(): (K, V) = mapped.next()

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

  /**
    * This is similar to apply(key) except it also applies row lock which is useful if the client
    * wants to make sure that the returned value incorporates all changes applied to it in update operations
    * with in the same sequence.
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

  private def apply(key: ByteBuffer): Option[V] = {
    for (
      cell: ByteBuffer <- option(storage.memstore(key));
      bytes: Array[Byte] <- option(storage.memstore.unwrap(key, cell, ttlMs))
    ) yield valueSerde.fromBytes(bytes)
  }

  /**
    * @return numKeys hint - this may or may not be accurate, depending on the underlying backend's features
    */
  def numKeys: Long = storage.memstore.numKeys()

  /**
    * replace is a faster operation than update because it doesn't look at the existing value
    * associated with the given key
    *
    * @param key   to update
    * @param value new value to be associated with the key
    * @return Unit Future which may be failed if the operation didn't succeed
    */
  def replace(key: K, value: V): Future[Unit] = {
    put(ByteBuffer.wrap(keySerde.toBytes(key)), value).map(__ => push(key, value))
  }

  /**
    * delete the given key
    *
    * @param key to delete
    * @return Unit Future which may be failed if the operation didn't succeed
    */
  def delete(key: K): Future[Unit] = {
    delete(ByteBuffer.wrap(keySerde.toBytes(key))).map(_ => push(key, null))
  }

  /**
    * update is a syntactic sugar for update where the value is always overriden
    *
    * @param key   to updateImpl
    * @param value new value to be associated with the key
    * @return Future Optional of the value previously held at the key position
    */
  def update(key: K, value: V): Future[Option[V]] = update(key) {
    case Some(prev) if prev == value => (None, Some(prev), Some(prev))
    case Some(prev) => (Some(value), Some(value), Some(prev))
    case None => (Some(value), Some(value), None)
  }

  /**
    * remove is a is a syntactic sugar for update where None is used as Value
    * it is different from delete in that it returns the removed value
    * which is more costly.
    *
    * @param key     to remove
    * @param command is the message that will be pushed to key-value observers
    * @return Future Optional of the value previously held at the key position
    */
  def remove(key: K, command: Any): Future[Option[V]] = update(key) {
    case None => (None, None, None)
    case Some(component) => (Some(command), None, Some(component))
  }

  /**
    * insert is a syntactic sugar for putImpl where the value is overriden if it doesn't exist
    * and the command is the value itself
    *
    * @param key   to insert
    * @param value new value to be associated with the key
    * @return Future Optional of the value previously held at the key position
    */
  def insert(key: K, value: V): Future[V] = {
    update(key) {
      case Some(_) => throw new IllegalArgumentException(s"$key already exists in state store")
      case None => (Some(value), Some(value), value)
    }
  }

  /**
    * update enables per-key observer pattern for incremental updates.
    *
    * @param key  key which is going to be updated
    * @param pf   putImpl function which maps the current value Option[V] at the given key to 3 values:
    *             1. Option[Any] is the incremental putImpl event
    *             2. Option[V] is the new state for the given key as a result of the incremntal putImpl
    *             3. R which is the result value expected by the caller
    * @return Future[R] which will be successful if the put operation of Option[V] of the pf succeeds
    */
  def update[R](key: K)(pf: PartialFunction[Option[V], (Option[Any], Option[V], R)]): Future[R] = {
    try {
      val k = ByteBuffer.wrap(keySerde.toBytes(key))
      val l = lock(key)
      try {
        pf(apply(k)) match {
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
    * at the given key. The value is first updated in the memstore and then a future is created
    * for reflecting the modification in the underlying storage. If the storage write fails
    * the previous value is rolled back in the memstore and the failure is propagated into
    * the result future.
    *
    * @param key   serielized key wrapped in a ByteBuffer
    * @param value new value for the key
    * @return future of the checkpoint that will represent the consistency information after the operation completes
    */
  private def put(key: ByteBuffer, value: V): Future[Checkpoint] = {
    val nowMs = System.currentTimeMillis()
    val recordTimestamp = value match {
      case e: EventTime => e.eventTimeUnix()
      case _ => nowMs
    }
    if (ttlMs > 0 && recordTimestamp + ttlMs < nowMs) {
      delete(key)
    } else {
      val valueBytes = valueSerde.toBytes(value)
      val record = new Record(ByteUtils.bufToArray(key), valueBytes, recordTimestamp)
      storage.write(record) map { offset =>
        val memStoreValue = storage.memstore.wrap(valueBytes, recordTimestamp)
        storage.memstore.put(key, memStoreValue, offset)
      }
    }
  }

  /**
    * An asynchronous non-blocking removal operation which deletes the value
    * at the given key. The value is first removed from the memstore and then a future is created
    * for reflecting the modification in the underlying storage. If the storage write fails
    * the previous value is rolled back in the memstore and the failure is propagated into
    * the result future.
    *
    * @param key serialized key to delete
    * @return future of the checkpoint that will represent the consistency information after the operation completes
    */
  private def delete(key: ByteBuffer): Future[Checkpoint] = {
    storage.delete(ByteUtils.bufToArray(key)) map { offset =>
      storage.memstore.remove(key, offset)
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
        case (key: Any, event: Any) => pf.lift(key.asInstanceOf[K], event)
        case illegal => throw new RuntimeException(s"Can't send $illegal to observers")
      }
    })
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

  override def internalPush(key: Array[Byte], value: Optional[Array[Byte]]) = {
    if (value.isPresent) {
      push(keySerde.fromBytes(key), Some(valueSerde.fromBytes(value.get)))
    } else {
      push(keySerde.fromBytes(key), None)
    }
  }
}
