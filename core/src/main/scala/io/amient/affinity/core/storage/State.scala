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
import java.util.{Observable, Observer}

import akka.actor.ActorSystem
import akka.serialization.{SerializationExtension, Serializer}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object State {
  val CONFIG_STATE = "affinity.state"

  def CONFIG_STATE_STORE(name: String) = s"affinity.state.$name"

  val CONFIG_STORAGE_CLASS = "storage.class"
  val CONFIG_MEMSTORE_CLASS = "memstore.class"
  val CONFIG_MEMSTORE_READ_TIMEOUT_MS = "memstore.read.timeout.ms"
}

class State[K: ClassTag, V: ClassTag](val name: String, system: ActorSystem, stateConfig: Config)
                                     (implicit val partition: Int) {

  private val serialization = SerializationExtension(system)

  def serde[S: ClassTag]: Serializer = {
    val cls = implicitly[ClassTag[S]].runtimeClass
    val serdeClass =
      if (cls == classOf[Boolean]) classOf[java.lang.Boolean]
      else if (cls == classOf[Byte]) classOf[java.lang.Byte]
      else if (cls == classOf[Int]) classOf[java.lang.Integer]
      else if (cls == classOf[Long]) classOf[java.lang.Long]
      else if (cls == classOf[Float]) classOf[java.lang.Float]
      else if (cls == classOf[Double]) classOf[java.lang.Double]
      else cls
    serialization.serializerFor(serdeClass)
  }

  import State._

  val config = stateConfig.withFallback(ConfigFactory.empty()
    .withValue(CONFIG_MEMSTORE_CLASS, ConfigValueFactory.fromAnyRef(classOf[MemStoreSimpleMap].getName))
    .withValue(CONFIG_MEMSTORE_READ_TIMEOUT_MS, ConfigValueFactory.fromAnyRef(1000))
  )

  val keySerde = serde[K]
  val valueSerde = serde[V]
  val readTimeout = config.getInt(CONFIG_MEMSTORE_READ_TIMEOUT_MS) milliseconds

  private val storageClass = Class.forName(config.getString(CONFIG_STORAGE_CLASS)).asSubclass(classOf[Storage])
  private val storageClassSymbol = rootMirror.classSymbol(storageClass)
  private val storageClassMirror = rootMirror.reflectClass(storageClassSymbol)
  private val constructor = storageClassSymbol.asClass.primaryConstructor.asMethod
  private val constructorMirror = storageClassMirror.reflectConstructor(constructor)

  val storage = constructorMirror(config, partition).asInstanceOf[Storage]

  import system.dispatcher

  /**
    * Retrieve a value from the store asynchronously
    *
    * @param key
    * @return Future.Success(Some(V)) if the key exists and the value could be retrieved and deserialized
    *         Future.Success(None) if the key doesn't exist
    *         Future.Failed(Throwable) if a non-fatal exception occurs
    */
  def apply(key: Any): Option[V] = {
    val k = ByteBuffer.wrap(keySerde.toBinary(key.asInstanceOf[AnyRef]))
    storage.memstore(k) map[V] {
      case d => valueSerde.fromBinary(d.array) match {
        case value: V => value
        case _ => throw new UnsupportedOperationException(key.toString)
      }
    }
  }


  def iterator: Iterator[(K, V)] = storage.memstore.iterator.map { case (mk, mv) =>
    (keySerde.fromBinary(mk.array()).asInstanceOf[K], valueSerde.fromBinary(mv.array).asInstanceOf[V])
  }

  def size: Long = storage.memstore.iterator.size

  /**
    * An asynchronous non-blocking put operation which inserts or updates the value
    * at the given key. The value is first updated in the memstore and then a future is created
    * for reflecting the modification in the underlying storage. If the storage write fails
    * the previous value is rolled back in the memstore and the failure is propagated into
    * the result future.
    *
    * @param key
    * @param value
    * @return A a future optional value previously held at the key position
    *         the future option will be equal to None if new a value was inserted
    */
  def put(key: K, value: V): Future[Option[V]] = {
    val k = ByteBuffer.wrap(keySerde.toBinary(key.asInstanceOf[AnyRef]))
    val write = if (value == null) null else ByteBuffer.wrap(valueSerde.toBinary(value.asInstanceOf[AnyRef]))
    storage.memstore.update(k, write) match {
      case Some(prev) if (prev == write) => Future.successful(Some(value))
      case differentOrNone =>
        writeWithMemstoreRollback(k, differentOrNone, storage.write(k, write))/* map {
          case prev => push(key, Some(value)); prev
        }*/
    }
  }

  /**
    * An asynchronous non-blocking removal operation which deletes the value
    * at the given key. The value is first removed from the memstore and then a future is created
    * for reflecting the modification in the underlying storage. If the storage write fails
    * the previous value is rolled back in the memstore and the failure is propagated into
    * the result future.
    *
    * @param key
    * @return A a future optional value previously held at the key position
    *         the future option will be equal to None if new a value was inserted
    */
  def remove(key: K): Future[Option[V]] = {
    val k = ByteBuffer.wrap(keySerde.toBinary(key.asInstanceOf[AnyRef]))
    storage.memstore.remove(k) match {
      case None => Future.successful(None)
      case some =>
        writeWithMemstoreRollback(k, some, storage.write(k, null))/* map {
          case prev => push(key, None); prev
        }*/
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
    def commit(success: Any) = prev.map(x => valueSerde.fromBinary(x.array).asInstanceOf[V])
    def revert(failure: Throwable) = {
      //write to storage failed - reverting the memstore modification
      //TODO use cell versioning or timestamp to cancel revert if another write succeeded after the one being reverted
      prev match {
        case None => storage.memstore.remove(k)
        case Some(rollback) => storage.memstore.update(k, rollback)
      }
      failure
    }
    write transform(commit, revert)
  }

  /*
   * Observer Support - the following code enables per-key observer pattern
   */

  def push(key: Any, event: Any): Unit = {
    observables.get(key).foreach(_.notifyObservers(event))
  }

  class ObservableState extends Observable {
    override def notifyObservers(arg: scala.Any): Unit = {
      //TODO with atomic cell versioning can cancel out redundant updates
      setChanged()
      super.notifyObservers(arg)
    }
  }

  private var observables = Map[Any, ObservableState]()

  def addObserver(key: Any, observer: Observer): Observer = {
    val observable = observables.get(key) match {
      case Some(observable) => observable
      case None =>
        val observable = new ObservableState()
        observables += key -> observable
        observable
    }
    observable.addObserver(observer)
    observer.update(observable, apply(key)) // send initial value on subscription
    observer
  }

  def removeObserver(key: Any, observer: Observer): Unit = {
    observables.get(key).foreach {
      observable => observable.deleteObserver(observer)
        if (observable.countObservers() == 0) observables -= key
    }
  }

}
