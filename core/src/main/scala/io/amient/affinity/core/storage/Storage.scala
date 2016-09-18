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
    * the contract of this method is that it should start a background process of restoring
    * the state from the underlying storage.
    */
  private[core] def init(): Unit

  /**
    * The implementation should stop listening for updates on the underlying topic after it has
    * fully caught up with the state updates. This method must block until completed and can
    * be interrupted by a consequent tail() call.
    *
    * Once this method returns, the partition or service which owns it will become available
    * for serving requests and so the state must be in a fully consistent state.
    *
    */
  private[core] def boot(): Unit

  /**
    * the implementation should start listening for updates and keep the memstore up to date.
    * The tailing must be done asychrnously - this method must return immediately and be idempotent.
    */
  private[core] def tail(): Unit

  /**
    * close all resource and background processes
    */
  private[core] def close(): Unit

  /**
    * Storage offers only simple blocking put so that the mutations do not escape single-threaded actor
    * context from which it is called
    * @param key
    * @param value if None is given as value the key will be removed from the underlying storage
    *              otherwise the key will be updated with the value
    */
  final def put(key: K, value: Option[V]): Unit = value match {
    case None => if (remove(key)) write(key, null.asInstanceOf[V]).get()
    case Some(data) => if (update(key, data)) write(key, data).get()
  }


  /**
    * @param key of the pair
    * @param value of the pair
    * @return Future with metadata returned by the underlying implementation
    */
  def write(key: K, value: V): java.util.concurrent.Future[_]

}
