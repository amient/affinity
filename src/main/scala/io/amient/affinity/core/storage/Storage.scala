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

trait Storage[K,V] extends MemStore[K, V] {

  /**
    * @param becomeMaster a function that can check whether this instance should become master, otherwise standby
    */
  def boot(becomeMaster: () => Boolean): Unit

  /**
    * Storage offers only simple blocking put so that the mutations do not escape single-threaded actor
    * context from which it is called
    * @param key
    * @param value if None is given as value the key will be removed from the underlying storage
    *              otherwise the key will be updated with the value
    */
  final def put(key: K, value: Option[V]): Unit = value match {
    case None => if (remove(key)) write(serialize(key, null.asInstanceOf[V])).get()
    case Some(data) => if (update(key, data)) write(serialize(key, data)).get()
  }


  /**
    * @param kv key value pair to write
    * @return Future with metadata returned by the underlying implementation
    */
  def write(kv: (Array[Byte], Array[Byte])): java.util.concurrent.Future[_]
  def serialize: (K,V) => (Array[Byte], Array[Byte])
  def deserialize: (Array[Byte], Array[Byte]) => (K,V)
}
