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
import akka.serialization.{JSerializer, SerializationExtension}
import com.typesafe.config.Config

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object State {
  def CONFIG_STATE(name: String) = s"affinity.state.$name"
  val CONFIG_STORAGE_CLASS = "storage.class"
  val CONFIG_MEMSTORE_CLASS = "memstore.class"
}

class State[K: ClassTag, V: ClassTag](system: ActorSystem, stateConfig: Config)(implicit val partition: Int) {

  private val serialization = SerializationExtension(system)

  //TODO verify that using JSerializer doesn't have performance impact becuase of primitive types casting to AnyRef
  def serde[S: ClassTag]: JSerializer = {
    val cls = implicitly[ClassTag[S]].runtimeClass
    val serdeClass =
      if (cls == classOf[Boolean]) classOf[java.lang.Boolean]
      else if (cls == classOf[Byte]) classOf[java.lang.Byte]
      else if (cls == classOf[Int]) classOf[java.lang.Integer]
      else if (cls == classOf[Long]) classOf[java.lang.Long]
      else if (cls == classOf[Float]) classOf[java.lang.Float]
      else if (cls == classOf[Double]) classOf[java.lang.Double]
      else cls
    serialization.serializerFor(serdeClass).asInstanceOf[JSerializer]
  }

  val keySerde = serde[K]
  val valueSerde = serde[V]

  private val storageClass = Class.forName(stateConfig.getString(State.CONFIG_STORAGE_CLASS)).asSubclass(classOf[Storage])
  private val storageClassSymbol = rootMirror.classSymbol(storageClass)
  private val storageClassMirror = rootMirror.reflectClass(storageClassSymbol)
  private val constructor = storageClassSymbol.asClass.primaryConstructor.asMethod
  private val constructorMirror = storageClassMirror.reflectConstructor(constructor)

  val storage = constructorMirror(stateConfig, partition).asInstanceOf[Storage]

  /**
    * Storage offers only simple blocking put so that the mutations do not escape single-threaded actor
    * context from which it is called
    *
    * @param key
    * @param value if None is given as value the key will be removed from the underlying storage
    *              otherwise the key will be updated with the value
    * @return An optional value previously held at the key position, None if new value was inserted
    */
  final def put(key: K, value: Option[V]): Option[V] = {
    val k = ByteBuffer.wrap(keySerde.toBinary(key.asInstanceOf[AnyRef]))
    value match {
      case None => storage.memstore.remove(k) map { prev =>
        storage.write(k, null).get()
        valueSerde.fromBinary(prev.array).asInstanceOf[V]
      }
      case Some(data) => {
        val v = ByteBuffer.wrap(valueSerde.toBinary(data.asInstanceOf[AnyRef]))
        storage.memstore.update(k, v) match {
          case Some(prev) if (prev == v) => Some(data)
          case other: Option[ByteBuffer] =>
            //TODO storage options: non-blocking variant should be available
            storage.write(k, v).get()
            other.map(x => valueSerde.fromBinary(x.array).asInstanceOf[V])
        }
      }
    }
  }

  final def get(key: K): Option[V] = {
    val k = ByteBuffer.wrap(keySerde.toBinary(key.asInstanceOf[AnyRef]))
    storage.memstore.get(k).map(d => valueSerde.fromBinary(d.array).asInstanceOf[V])
  }

  def iterator: Iterator[(K, V)] = storage.memstore.iterator.map { case (mk, mv) =>
    (keySerde.fromBinary(mk.array()).asInstanceOf[K], valueSerde.fromBinary(mv.array).asInstanceOf[V])
  }

  def size: Long = storage.memstore.size

}
