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

import akka.actor.ActorSystem
import akka.serialization.{JSerializer, SerializationExtension, Serializer}
import com.typesafe.config.Config

import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object State {
  val CONFIG_STATE = "affinity.state"
  def CONFIG_STATE_STORE(name: String) = s"affinity.state.$name"

  val CONFIG_STORAGE_CLASS = "storage.class"
  val CONFIG_MEMSTORE_CLASS = "memstore.class"
}

class State[K: ClassTag, V: ClassTag](system: ActorSystem, stateConfig: Config)(implicit val partition: Int) {

  private val serialization = SerializationExtension(system)

  //TODO verify that using JSerializer doesn't have performance impact becuase of primitive types casting to AnyRef
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

  val keySerde = serde[K]
  val valueSerde = serde[V]

  private val storageClass = Class.forName(stateConfig.getString(State.CONFIG_STORAGE_CLASS)).asSubclass(classOf[Storage])
  private val storageClassSymbol = rootMirror.classSymbol(storageClass)
  private val storageClassMirror = rootMirror.reflectClass(storageClassSymbol)
  private val constructor = storageClassSymbol.asClass.primaryConstructor.asMethod
  private val constructorMirror = storageClassMirror.reflectConstructor(constructor)

  val storage = constructorMirror(stateConfig, partition).asInstanceOf[Storage]

  import system.dispatcher

  /**
    * Retrieve a value from the store asynchronously
    *
    * @param key
    * @return Future.Success(V) if the key exists and the value could be retrieved and deserialized
    *         Future.Failed(UnsupportedOperationException) if the key exists but the value class is not registered
    *         Future.Failed(NoSuchElementException) if the key doesn't exist
    *         Future.Failed(Throwable) if any other non-fatal exception occurs
    */
  def apply(key: K): Future[V] = {
    val k = ByteBuffer.wrap(keySerde.toBinary(key.asInstanceOf[AnyRef]))
    storage.memstore(k) flatMap {
      case d => valueSerde.fromBinary(d.array) match {
        case value: V => Future.successful(value)
        case _ => Future.failed(new UnsupportedOperationException(key.toString))
      }
    }
  }

  def iterator: Iterator[(K, V)] = storage.memstore.iterator.map { case (mk, mv) =>
    (keySerde.fromBinary(mk.array()).asInstanceOf[K], valueSerde.fromBinary(mv.array).asInstanceOf[V])
  }

  def size: Long = storage.memstore.size

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
    val write = if (value == null) null else ByteBuffer.wrap (valueSerde.toBinary (value.asInstanceOf[AnyRef]))
    storage.memstore.update (k, write) match {
      case Some(prev) if (prev == write) => Future.successful(Some(value))
      case differentOrNone => writeWithMemstoreRollback(k, differentOrNone, storage.write(k, write))
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
      case differentOrNone =>  writeWithMemstoreRollback(k, differentOrNone, storage.write(k, null))
    }
  }

  private def writeWithMemstoreRollback(k: ByteBuffer, prev: Option[ByteBuffer], write: Future[_]): Future[Option[V]] = {
    write transform (
      (success) => prev.map(x => valueSerde.fromBinary(x.array).asInstanceOf[V]),
      (failure: Throwable) => {
        //write to storage failed - reverting the memstore modification
        //TODO use cell versioning or timestamp to cancel revert if another write succeeded in the mean-time
        prev match {
          case None => storage.memstore.remove(k)
          case Some(rollback) => storage.memstore.update(k, rollback)
        }
        failure
      }
      )
  }
}
