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

package io.amient.affinity.core.actor

import akka.actor.{ActorContext, ActorRef, Props}
import akka.util.Timeout
import io.amient.affinity.core.ack
import io.amient.affinity.core.util.Reply

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

case class TransactionBegin() extends Reply[Unit]

case class TransactionCommit() extends Reply[Unit]

case class TransactionAbort() extends Reply[Unit]

case class TransactionalRecord(topic: String,
                               key: Array[Byte],
                               value: Array[Byte],
                               timestamp: Option[Long],
                               partition: Option[Int]) extends Reply[Comparable[_]]


class TransactionCoordinator(context: ActorContext) {

  @volatile private var _isInTrnsaction: Boolean = false

  def inTransaction(): Boolean = _isInTrnsaction

  val t = 1 minute //FIXME

  lazy val producer: ActorRef = context.actorOf(Props(Class.forName("io.amient.affinity.kafka.TransactionalProducer")))

  import context.dispatcher

  def begin(): Unit = {
    implicit val timeout = Timeout(t)
    Await.result(producer ?? TransactionBegin(), t)
    _isInTrnsaction = true
  }

  def append(topic: String, key: Array[Byte], value: Array[Byte], timestamp: Option[Long], partition: Option[Int]): Future[_ <: Comparable[_]] = {
    implicit val timeout = Timeout(t)
    producer ?? TransactionalRecord(topic, key, value, timestamp, partition)
  }

  def commit(): Unit = try {
    implicit val timeout = Timeout(t)
    Await.result(producer ?? TransactionCommit(), t)
  } finally {
    _isInTrnsaction = false
  }

  def abort(): Unit = try {
    implicit val timeout = Timeout(t)
    Await.result(producer ?? TransactionAbort(), t)
  } finally {
    _isInTrnsaction = false
  }

}


