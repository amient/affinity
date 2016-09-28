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

import com.typesafe.config.Config
import io.amient.affinity.core.serde.Serde

object State {
  def CONFIG_STORAGE_CLASS(name: String) = s"affinity.state.$name.storage.class"
  def CONFIG_MEMSTORE_CLASS(name: String) = s"affinity.state.$name.memstore.class"
  def CONFIG_KEY_SERDE(name: String) = s"affinity.state.$name.key.serde"
  def CONFIG_VALUE_SERDE(name: String) = s"affinity.state.$name.value.serde"
}

abstract class State[K, V](config: Config) {

  val storage: Storage

  val keySerde: Serde

  val valueSerde: Serde

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
    val k = ByteBuffer.wrap(keySerde.toBytes(key))
    value match {
      case None => storage.memstore.remove(k) map { prev =>
        storage.write(k, null).get()
        valueSerde.fromBytes(prev.array).asInstanceOf[V]
      }
      case Some(data) => {
        val v = ByteBuffer.wrap(valueSerde.toBytes(data))
        storage.memstore.update(k, v) match {
          case Some(prev) if (prev == v) => Some(data)
          case other: Option[ByteBuffer] =>
            //TODO storage options: non-blocking variant should be available
            storage.write(k, v).get()
            other.map(x => valueSerde.fromBytes(x.array).asInstanceOf[V])
        }
      }
    }
  }

  final def get(key: K): Option[V] = {
    val k = ByteBuffer.wrap(keySerde.toBytes(key))
    storage.memstore.get(k).map(d => valueSerde.fromBytes(d.array).asInstanceOf[V])
  }

  def iterator: Iterator[(K, V)] = storage.memstore.iterator.map { case (mk, mv) =>
    (keySerde.fromBytes(mk.array()).asInstanceOf[K], valueSerde.fromBytes(mv.array).asInstanceOf[V])
  }

  def size: Long = storage.memstore.size

}
