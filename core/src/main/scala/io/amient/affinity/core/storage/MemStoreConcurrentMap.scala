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

import java.util.concurrent.ConcurrentHashMap

trait MemStoreConcurrentMap extends MemStore {

  private val internal = new ConcurrentHashMap[MK, MV]()

  override def get(key: MK): Option[MV] = internal.get(key) match {
    case null => None
    case other => Some(other)
  }

  override def iterator = new Iterator[(MK,MV)] {
    val it = internal.entrySet().iterator()

    override def hasNext: Boolean = it.hasNext

    override def next(): (MK, MV) = it.next match {
      case entry => (entry.getKey, entry.getValue)
    }
  }

  override def size: Long = internal.size

  override protected def update(key: MK, value: MV): Option[MV] = internal.put(key, value) match {
    case null => None
    case prev => Some(prev)
  }

  override protected def remove(key: MK): Option[MV] = internal.remove(key)  match {
    case null => None
    case prev => Some(prev)
  }
}