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

import akka.actor.{ActorRef, Scheduler}
import akka.util.Timeout
import io.amient.affinity.core.ack

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.reflect.ClassTag
import scala.util.{Failure, Success}
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.language.postfixOps

/**
  *
  * This is an asynchronous Transaction utility which works with Futures.
  * This is not a 2-phase commit transaction. Under a failure scenario, the best effort is made to reverse the partial
  * changes that have succeeded.
  */

object Transaction {

  def apply[T](default: ActorRef)(t: (Transaction) => Future[T])(implicit context: ExecutionContext) = {
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

  def execute[T: ClassTag](read: Future[T])(implicit context: ExecutionContext): Future[T] = {
    val promise = Promise[T]()
    implicit val timeout = Timeout(6 seconds) // FIXME this timeout has to be passed via arg or implicit
    read onComplete {
      case Success(result) =>
        //System.err.println(s"SUCCESS $result")
        promise.success(result)
      case Failure(e) =>
        //System.err.println(s"FAILURE ${e.getMessage}")
        promise.failure(e)
    }
    promise.future
  }

  def execute[T: ClassTag](instr: Instruction[T])(implicit context: ExecutionContext, scheduler: Scheduler): Future[T] = {
    execute(default, instr)
  }

  def execute[T: ClassTag](actor: ActorRef, instr: Instruction[T])(implicit context: ExecutionContext, scheduler: Scheduler): Future[T] = {
    val promise = Promise[T]()
    implicit val timeout = Timeout(6 seconds) // FIXME this timeout has to be passed via arg or implicit
    actor ack[T](instr) onComplete {
      case Success(result: T) =>
        //System.err.println(s"SUCCESS $instr")
        stack = stack :+ CompletedInstruction(actor, instr, result)
        promise.success(result)
      case Failure(e) =>
        //System.err.println(s"FAILURE $instr: $e")
        promise.failure(e)
    }
    promise.future
  }
}
