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
import java.util
import java.util.Map.Entry
import java.util.Optional
import java.util.concurrent.ConcurrentHashMap

import com.typesafe.config.Config

class MemStoreSimpleMap(config: Config, partition: Int) extends MemStore(config, partition) {

  private val internal = new ConcurrentHashMap[ByteBuffer, ByteBuffer]()

  override def close(): Unit = internal.clear()

  override def iterator(): util.Iterator[Entry[ByteBuffer, ByteBuffer]] = {
    internal.entrySet().iterator()
  }

  override def apply(key: ByteBuffer): Optional[ByteBuffer] = {
    Optional.ofNullable(internal.get(key))
  }

  override def putImpl(key: ByteBuffer, value: ByteBuffer): Boolean = {
    internal.put(key, value) == null
  }

  override def removeImpl(key: ByteBuffer):Boolean = {
    internal.remove(key) != null
  }

  override protected def isPersistent = false
}
