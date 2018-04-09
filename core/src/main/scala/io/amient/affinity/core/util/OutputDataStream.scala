/*
 * Copyright 2016-2018 Michal Harish, michal.harish@gmail.com
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

package io.amient.affinity.core.util

import io.amient.affinity.core.serde.AbstractSerde
import io.amient.affinity.core.storage.{LogStorage, LogStorageConf, Record}

import scala.concurrent.{ExecutionContext, Future}
import scala.language.existentials

class OutputDataStream[K, V](keySerde: AbstractSerde[_ >: K], valSerde: AbstractSerde[_ >: V], conf: LogStorageConf) {

  lazy val storage = LogStorage.newInstance(conf)

  def append(record: Record[K, V]): Future[_ <: Comparable[_]] = {
    val binaryRecord = new Record(keySerde.toBytes(record.key), valSerde.toBytes(record.value), record.timestamp)
    val jf = storage.append(binaryRecord)
    Future(jf.get)(ExecutionContext.Implicits.global)
  }

  def delete(key: K): Future[_ <: Comparable[_]] = {
    val jf = storage.delete(keySerde.toBytes(key))
    Future(jf.get)(ExecutionContext.Implicits.global)
  }

  def flush(): Unit = storage.flush()

  def close(): Unit = {
    try flush() finally try storage.close() finally {
      keySerde.close()
      valSerde.close()
    }
  }
}
