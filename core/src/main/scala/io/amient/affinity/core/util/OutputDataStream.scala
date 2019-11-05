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

import akka.util.Timeout
import io.amient.affinity.core.actor.TransactionCoordinator
import io.amient.affinity.core.serde.AbstractSerde
import io.amient.affinity.core.storage.{LogStorage, LogStorageConf, Record}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.{existentials, postfixOps}


object OutputDataStream {

  class TransactionCoordinatorNoop extends TransactionCoordinator {
    override def _begin(): Future[Unit] = Future.successful(())

    override def _commit(): Future[Unit] = Future.successful(())

    override def _abort(): Future[Unit] = Future.successful(())

    override def append(topic: String, key: Array[Byte], value: Array[Byte], timestamp: Option[Long], partition: Option[Int]): Future[_ <: Comparable[_]] = {
      Future.successful(0L)
    }
  }

  //create OutputDataStream without transactional support
  def apply[K, V](keySerde: AbstractSerde[_ >: K], valSerde: AbstractSerde[_ >: V], conf: LogStorageConf): OutputDataStream[K, V] = {
    new OutputDataStream[K, V](new TransactionCoordinatorNoop, keySerde, valSerde, conf)
  }

}

class OutputDataStream[K, V] private[affinity](txn: TransactionCoordinator, keySerde: AbstractSerde[_ >: K], valSerde: AbstractSerde[_ >: V], conf: LogStorageConf) {

  lazy val storage = LogStorage.newInstanceEnsureExists(conf)

  lazy private val topic: String = storage.getTopic()

  implicit val timeout = Timeout(1 minute) //FIXME

  def append(record: Record[K, V]): Future[_ <: Comparable[_]] = {
    if (txn.inTransaction()) {
      txn.append(topic, keySerde.toBytes(record.key), valSerde.toBytes(record.value), Option(record.timestamp), None)
    } else {
      val binaryRecord = new Record(keySerde.toBytes(record.key), valSerde.toBytes(record.value), record.timestamp)
      val jf = storage.append(binaryRecord)
      Future(jf.get)(ExecutionContext.Implicits.global)
    }
  }

  def delete(key: K): Future[_ <: Comparable[_]] = {
    if (txn.inTransaction()) {
      txn.append(topic, keySerde.toBytes(key), null, None, None)
    } else {
      val jf = storage.delete(keySerde.toBytes(key))
      Future(jf.get)(ExecutionContext.Implicits.global)
    }
  }

  def flush(): Unit = storage.flush()

  def close(): Unit = {
    try flush() finally try storage.close() finally {
      keySerde.close()
      valSerde.close()
    }
  }
}


