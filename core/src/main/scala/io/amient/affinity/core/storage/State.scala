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
import java.util.{Observable, Observer, Optional}

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import io.amient.affinity.core.serde.Serde

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.{existentials, postfixOps}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.control.NonFatal

object State {
  val CONFIG_STATE = "affinity.state"

  def CONFIG_STATE_STORE(name: String) = s"affinity.state.$name"

  val CONFIG_TTL_SECONDS = "ttl.sec"
  val CONFIG_STORAGE_CLASS = "storage.class"
  val CONFIG_MEMSTORE_CLASS = "memstore.class"
  val CONFIG_MEMSTORE_READ_TIMEOUT_MS = "memstore.read.timeout.ms"
}

class State[K: ClassTag, V: ClassTag](val name: String, system: ActorSystem, stateConfig: Config)
                                     (implicit val partition: Int) extends Observable {

  self =>

  import State._

  private val config = stateConfig.withFallback(ConfigFactory.empty()
    .withValue(CONFIG_MEMSTORE_CLASS, ConfigValueFactory.fromAnyRef(classOf[MemStoreSimpleMap].getName))
    .withValue(CONFIG_MEMSTORE_READ_TIMEOUT_MS, ConfigValueFactory.fromAnyRef(1000))
  )

  private val keySerde = Serde.of[K](system.settings.config)
  private val valueSerde = Serde.of[V](system.settings.config)

  private val recordTtlMs = if (config.hasPath(CONFIG_TTL_SECONDS)) {
    config.getLong(CONFIG_TTL_SECONDS) * 1000
  } else {
    Long.MaxValue
  }
  private val readTimeout = config.getInt(CONFIG_MEMSTORE_READ_TIMEOUT_MS) milliseconds

  private val storageClass = Class.forName(config.getString(CONFIG_STORAGE_CLASS)).asSubclass(classOf[Storage])
  private val storageClassSymbol = rootMirror.classSymbol(storageClass)
  private val storageClassMirror = rootMirror.reflectClass(storageClassSymbol)
  private val constructor = storageClassSymbol.asClass.primaryConstructor.asMethod
  private val constructorMirror = storageClassMirror.reflectConstructor(constructor)

  val storage: Storage = try constructorMirror(config, partition).asInstanceOf[Storage] catch {
    case NonFatal(e) => throw new RuntimeException(s"Failed to Configure State $name", e)
  }

  import system.dispatcher

  def option[T](opt: Optional[T]): Option[T] = if (opt.isPresent) Some(opt.get()) else None

  /**
    * Retrieve a value from the store asynchronously
    *
    * @param key to retrieve value of
    * @return Future.Success(Some(V)) if the key exists and the value could be retrieved and deserialized
    *         Future.Success(None) if the key doesn't exist
    *         Future.Failed(Throwable) if a non-fatal exception occurs
    */
  def apply(key: K): Option[V] = {
    val k: ByteBuffer = ByteBuffer.wrap(keySerde.toBytes(key))
    for (
      cell: ByteBuffer <- option(storage.memstore(k));
      bytes: Array[Byte] <- option(storage.memstore.unwrap(k, cell, recordTtlMs))
    ) yield valueSerde.fromBytes(bytes) match {
      case value: V => value
      case other => throw new UnsupportedOperationException(key.toString + " " + other.getClass)
    }
  }

  def iterator: Iterator[(K, V)] = {
    storage.memstore.iterator.asScala.flatMap { entry =>
      val key = keySerde.fromBytes(entry.getKey.array())
      option(storage.memstore.unwrap(entry.getKey(), entry.getValue, recordTtlMs)).map {
        bytes => (key, valueSerde.fromBytes(bytes))
      }
    }
  }

  def size: Long = {
    val it = storage.memstore.iterator
    var count = 0
    while (it.hasNext) {
      count += 1
      it.next()
    }
    count
  }

  /**
    * insert is a syntactic sugar for update where the value is overriden if it doesn't exist
    * and the command is the value itself
    *
    * @param key to insert
    * @param value new value to be associated with the key
    * @return Future Optional of the value previously held at the key position
    */
  def insert(key: K, value: V): Future[V] = update(key) {
    case Some(_) => throw new IllegalArgumentException(s"$key already exists in state store `$name`")
    case None => (Some(value), Some(value), value)
  }

  /**
    * update is a syntactic sugar for update where the value is always overriden
    *
    * @param key to update
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
    *
    * @param key to remove
    * @param command is the message that will be pushed to key-value observers
    * @return Future Optional of the value previously held at the key position
    */
  def remove(key: K, command: Any): Future[Option[V]] = update(key) {
    case None => (None, None, None)
    case Some(component) => (Some(command), None, Some(component))
  }

  /**
    * update enables per-key observer pattern for incremental updates
    *
    * @param key  key which is going to be updated
    * @param pf   update function which maps the current value Option[V] at the given key to 3 values:
    *             1. Option[Any] is the incremental update event
    *             2. Option[V] is the new state for the given key as a result of the incremntal update
    *             3. R which is the result value expected by the caller
    * @return Future[R] which will be successful if the put operation of Option[V] of the pf succeeds
    */
  def update[R](key: K)(pf: PartialFunction[Option[V], (Option[Any], Option[V], R)]): Future[R] = {
    try {
      pf(apply(key)) match {
        case (None, _, result) => Future.successful(result)
        case (Some(increment), changed, result) => changed match {
          case Some(updatedValue) =>
            put(key, updatedValue) andThen {
              case _ => push(key, increment)
            } map (_ => result)
          case None =>
            delete(key) andThen {
              case _ => push(key, increment)
            } map (_ => result)
        }
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
    * @param key to replace
    * @param value new value for the key
    * @return A a future optional value previously held at the key position
    *         the future option will be equal to None if new a value was inserted
    */
  private def put(key: K, value: V): Future[Option[V]] = {
    val nowMs = System.currentTimeMillis()
    val recordTimestamp = value match {
      case e: EventTime => e.eventTimeMs()
      case _ => nowMs
    }
    val expires = recordTimestamp + recordTtlMs
    if (recordTtlMs < Long.MaxValue && expires < nowMs) {
      delete(key)
    } else {
      val keyBytes = keySerde.toBytes(key)
      val k = ByteBuffer.wrap(keyBytes)
      val valueBytes = valueSerde.toBytes(value)
      val memStoreValue = storage.memstore.wrap(valueBytes, recordTimestamp)
      option(storage.memstore.update(k, memStoreValue)) match {
        case Some(prev) if prev == memStoreValue => Future.successful(Some(value))
        case differentOrNone =>
          val javaToScalaFuture = Future(storage.write(keyBytes, valueBytes, recordTimestamp).get)
          writeWithMemstoreRollback(k, differentOrNone, javaToScalaFuture)
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
    * @param key to delete
    * @return A a future optional value previously held at the key position
    *         the future option will be equal to None if new a value was inserted
    */
  private def delete(key: K): Future[Option[V]] = {
    val keyBytes = keySerde.toBytes(key)
    val k = ByteBuffer.wrap(keyBytes)
    option(storage.memstore.remove(k)) match {
      case None => Future.successful(None)
      case some =>
        val javaToScalaFuture = Future(storage.delete(keyBytes).get)
        writeWithMemstoreRollback(k, some, javaToScalaFuture)
    }
  }

  /**
    *
    * @param k     serialized key
    * @param prev  serialized bytes held at the given key before the write operation was invoked
    * @param write write operation which is expected to succeed otherwise the memstore will be reverted to the prev value
    * @return Future.Success(Option[ByteBuffer]) deserialized value previously held at the given key
    *         Future.Failure(Throwable) if the failure occurs
    */
  private def writeWithMemstoreRollback(k: ByteBuffer, prev: Option[ByteBuffer], write: Future[_]): Future[Option[V]] = {

    def commit(success: Any): Option[V] = prev
      .flatMap(cell => option(storage.memstore.unwrap(k, cell, recordTtlMs)))
      .map(valueSerde.fromBytes)

    def revert(failure: Throwable) = {
      //write to storage failed - reverting the memstore modification
      //TODO use cell versioning to cancel revert if another write succeeded after the one being reverted
      prev match {
        case None => storage.memstore.remove(k)
        case Some(rollback) => storage.memstore.update(k, rollback)
      }
      failure
    }

    write transform(commit, revert)
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
        case (key: K, event: Any) => pf.lift((key, event))
        case _ =>
      }
    })
  }

  /**
    * Observables are attached to individual keys in this State
    */
  private var observables = scala.collection.mutable.Map[K, ObservableKeyValue]()

  def push(key: K, event: Any): Unit = {
    try {
      observables.get(key).foreach(_.notifyObservers(event))
    } finally {
      setChanged()
      notifyObservers((key, event))
    }
  }

  class ObservableKeyValue extends Observable {
    override def notifyObservers(arg: scala.Any): Unit = {
      //TODO with atomic cell versioning we could cancel out redundant updates
      setChanged()
      super.notifyObservers(arg)
    }
  }

  def addKeyValueObserver(key: Any, observer: Observer): Observer = key match {
    case k: K =>
      val observable = observables.get(k) match {
        case Some(o) => o
        case None =>
          val o: ObservableKeyValue = new ObservableKeyValue()
          observables += k -> o
          o
      }
      observable.addObserver(observer)
      observer.update(observable, apply(k)) // send initial value on subscription
      observer
  }

  def removeKeyValueObserver(key: Any, observer: Observer): Unit = key match {
    case k: K => observables.get(k).foreach {
      observable =>
        observable.deleteObserver(observer)
        if (observable.countObservers() == 0) observables -= k
    }
  }

}
