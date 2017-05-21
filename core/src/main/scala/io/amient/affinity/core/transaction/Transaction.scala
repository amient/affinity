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

package io.amient.affinity.core.transaction

import java.util.concurrent.TimeoutException

import akka.actor.{ActorRef, Scheduler}
import akka.pattern.after
import akka.util.Timeout
import io.amient.affinity.core.ack
import io.amient.affinity.core.util.Reply

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.{implicitConversions, postfixOps}
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

/**
  *
  * This is an asynchronous Transaction utility which works with Futures.
  * This is not a 2-phase commit transaction. Under a failure scenario, the best effort is made to reverse the partial
  * changes that have succeeded.
  */

object Transaction {

  def apply[T](default: ActorRef)(t: (Transaction) => Future[T])(implicit context: ExecutionContext): Future[T] = {
    val transaction = new Transaction(default)
    val result = t(transaction)
    result transform((result: T) => result, (e: Throwable) => {
      transaction.rollback; e
    })
  }
}

class Transaction(default: ActorRef) {

  case class CompletedInstruction[T](actor: ActorRef, instr: Instruction[T], result: T) {
    def reverse = instr.reverse(result)
  }

  @volatile private var stack = List[CompletedInstruction[_]]()

  def rollback: Unit = {
    stack.foreach { case (completed: CompletedInstruction[_]) =>
      completed.reverse.foreach {
        case reversal =>
          //System.err.println("REVERTING " + completed.instr + " WITH " + reversal)
          completed.actor ! reversal
      }
    }
  }

  def apply[T: ClassTag](f: Future[T])(implicit timeout: Timeout, context: ExecutionContext, scheduler: Scheduler): Future[T] = {
    val promise = Promise[T]()

    lazy val t = after(timeout.duration, scheduler) (Future.failed(
      new TimeoutException(s"Transaction future timed out after ${timeout.duration}")))

    Future firstCompletedOf Seq(f, t) onComplete {
      case Success(result) =>
        //System.err.println(s"SUCCESS $result")
        promise.success(result)
      case Failure(e) =>
        //System.err.println(s"FAILURE ${e.getMessage}")
        promise.failure(e)
    }
    promise.future
  }

  def query[T: ClassTag](q: Reply[T])(implicit timeout: Timeout, context: ExecutionContext, scheduler: Scheduler): Future[T] = {
    query(default, q)
  }

  def query[T: ClassTag](actor: ActorRef, q: Reply[T])(implicit timeout: Timeout, context: ExecutionContext, scheduler: Scheduler): Future[T] = {
    val promise = Promise[T]()
    actor ack q onComplete {
      case Success(result) =>
        //System.err.println(s"SUCCESS $result")
        promise.success(result)
      case Failure(e) =>
        //System.err.println(s"FAILURE ${e.getMessage}")
        promise.failure(e)
    }
    promise.future
  }

  def execute[T: ClassTag](cmd: Instruction[T])(implicit timeout: Timeout, context: ExecutionContext, scheduler: Scheduler): Future[T] = {
    execute(default, cmd)
  }

  def execute[T: ClassTag](actor: ActorRef, cmd: Instruction[T])(implicit timeout: Timeout, context: ExecutionContext, scheduler: Scheduler): Future[T] = {
    val promise = Promise[T]()
    actor ack[T](cmd) onComplete {
      case Success(result: T) =>
        //System.err.println(s"SUCCESS $instr")
        stack = stack :+ CompletedInstruction(actor, cmd, result)
        promise.success(result)
      case Failure(e) =>
        //System.err.println(s"FAILURE $instr: $e")
        promise.failure(e)
    }
    promise.future
  }
}
