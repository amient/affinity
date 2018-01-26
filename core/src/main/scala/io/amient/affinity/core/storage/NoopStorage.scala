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

import java.util.concurrent.{CompletableFuture, Future}

import io.amient.affinity.stream.Record

class NoopStorage(id: String, conf: StateConf, partition:Int, numParts: Int) extends Storage(id, conf, partition) {

  final val readonly: Boolean = conf.External()

  override def init(state: ObservableState[_]): Unit = ()

  override def boot(): Unit = ()

  override def tail(): Unit = ()

  override def write(record: Record[Array[Byte], Array[Byte]]): Future[java.lang.Long] = {
    if (readonly) throw new RuntimeException("Modification attempt on a readonly storage")
    CompletableFuture.completedFuture((-1L))
  }

  override def delete(key: Array[Byte]): Future[java.lang.Long] = {
    if (readonly) throw new RuntimeException("Modification attempt on a readonly storage")
    CompletableFuture.completedFuture((-1L))
  }
}
