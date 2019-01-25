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
import io.amient.affinity.Conf
import io.amient.affinity.core.ack
import io.amient.affinity.core.util.Reply

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

case class ControllerBeginTransaction() extends Reply[Unit]

case class TransactionBegin(transactionalId: String) extends Reply[Unit]

case class TransactionCommit() extends Reply[Unit]

case class TransactionAbort() extends Reply[Unit]

case class TransactionalRecord(topic: String,
                               key: Array[Byte],
                               value: Array[Byte],
                               timestamp: Option[Long],
                               partition: Option[Int]) extends Reply[Comparable[_]]

trait TransactionCoordinator {
  @volatile private var _isInTrnsaction: Boolean = false

  def inTransaction(): Boolean = _isInTrnsaction

  implicit val executor = scala.concurrent.ExecutionContext.global

  final def begin(): Future[Unit] = _begin().transform(
    (s) => { _isInTrnsaction = false; s },
    (f) => { _isInTrnsaction = false; f})
  def _begin(): Future[Unit]

  /**
    * this doesn't have to block and should guarantee the order
    * @param topic
    * @param key
    * @param value
    * @param timestamp
    * @param partition
    * @return
    */
  def append(topic: String, key: Array[Byte], value: Array[Byte], timestamp: Option[Long], partition: Option[Int]): Future[_ <: Comparable[_]]

  final def commit(): Future[Unit] = _commit().transform(
    (s) => { _isInTrnsaction = false; s },
    (f) => { _isInTrnsaction = false; f})

  def _commit (): Future[Unit]

  def abort(): Future[Unit] = _abort().transform(
    (s) => { _isInTrnsaction = false; s },
    (f) => { _isInTrnsaction = false; f})
  def _abort(): Future[Unit]

}

class TransactionCoordinatorActor(context: ActorContext) extends TransactionCoordinator {
  val conf = Conf(context.system.settings.config)
  //TODO create a separate timout configuration because startup timeouts may too long for transactions
  val t = conf.Affi.Node.StartupTimeoutMs().toLong milliseconds
  val controller: ActorRef = Await.result(context.system.actorSelection("/user/controller").resolveOne()(Timeout(t)), t)

  implicit val timeout = Timeout(t)

  override def _begin(): Future[Unit] = controller ?? ControllerBeginTransaction()

  def append(topic: String, key: Array[Byte], value: Array[Byte], timestamp: Option[Long], partition: Option[Int]): Future[_ <: Comparable[_]] = {
    controller ?? TransactionalRecord(topic, key, value, timestamp, partition)
  }

  def _commit(): Future[Unit] = controller ?? TransactionCommit()

  def _abort(): Future[Unit] = controller ?? TransactionAbort()

}

class TransactionCoordinatorImpl(context: ActorContext, transactionalId: Option[String]) extends TransactionCoordinator {

  lazy val producer: ActorRef = context.actorOf(Props(Class.forName("io.amient.affinity.kafka.TransactionalProducer")))

  lazy val t = Conf(context.system.settings.config).Affi.Node.StartupTimeoutMs().toLong milliseconds

  def _begin(): Future[Unit] = {
    implicit val timeout = Timeout(t)
    producer ?? TransactionBegin(transactionalId.get)
  }

  def append(topic: String, key: Array[Byte], value: Array[Byte], timestamp: Option[Long], partition: Option[Int]): Future[_ <: Comparable[_]] = {
    implicit val timeout = Timeout(t)
    producer ?? TransactionalRecord(topic, key, value, timestamp, partition)
  }

  def _commit(): Future[Unit] = {
    implicit val timeout = Timeout(t)
    producer ?? TransactionCommit()
  }

  def _abort(): Future[Unit] = {
    implicit val timeout = Timeout(t)
    producer ?? TransactionAbort()
  }

}


