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

class MemStoreSimpleMap extends MemStore {

  private val internal = new util.HashMap[ByteBuffer, ByteBuffer]()

  override def iterator(): util.Iterator[Entry[ByteBuffer, ByteBuffer]] = {
    internal.entrySet().iterator()
  }

  override def apply(key: ByteBuffer): Optional[ByteBuffer] = {
    Optional.ofNullable(internal.get(key))
  }

  override def update(key: ByteBuffer, value: ByteBuffer): Optional[ByteBuffer] = {
    Optional.ofNullable(internal.put(key, value))
  }

  override def remove(key: ByteBuffer): Optional[ByteBuffer] = {
    Optional.ofNullable(internal.remove(key))
  }

  override def close(): Unit = {
    internal.clear()
  }
}
